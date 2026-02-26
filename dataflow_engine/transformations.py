"""
Configuration-driven Spark DataFrame transformations (no Spark SQL).

Each step is implemented with the Spark DataFrame API:
- filter, join, groupBy/agg, select/withColumn, orderBy, unionByName.
The runner calls apply_transformation_step(step, datasets) for each step;
sources are resolved by name from the datasets registry and the result
is stored by output_alias for the next step.
"""

import logging
import re
from typing import Any

from pyspark.sql import DataFrame
from pyspark.sql import functions as F

LOG = logging.getLogger(__name__)


def _col(name: str) -> str:
    """Convert COBOL field name (hyphens) to Spark column name (underscores)."""
    return name.replace("-", "_")


def _resolve_col(df: DataFrame, name: str) -> str:
    """Resolve column name in df; COBOL uses hyphens, Spark uses underscores.
    If no exact match, try columns from expressions (e.g. 'WS_DEBIT_TOTAL + TXN_AMT'
    when asking for 'WS_DEBIT_TOTAL') so config JSON works when steps reference
    expression-created columns."""
    c = _col(name)
    cols = [x for x in df.columns if x.replace("-", "_") == c]
    if cols:
        return cols[0]
    # Fallback: Spark may name expression results like "COL_A + COL_B"
    candidates = [
        x for x in df.columns
        if (x.replace("-", "_").startswith(c + " ") or
            x.replace("-", "_").startswith(c + "+") or
            x.replace("-", "_") == c)
    ]
    return candidates[0] if len(candidates) == 1 else c


def _column_exists(df: DataFrame, name: str) -> bool:
    """True if a column matching name (or resolved name) exists in df.
    Used for working-storage: mainframe temp variables may not exist yet."""
    resolved = _resolve_col(df, name)
    return resolved in df.columns


def _expression_to_column(df: DataFrame, expr_str: str, op: str = "move"):
    """Turn an expression string into a Spark Column: literal (number/quoted string) or column reference.
    Used for MOVE so that '16' or \"0\" becomes F.lit(16) / F.lit(0), not F.col('16')."""
    if not expr_str or not isinstance(expr_str, str):
        return F.lit(None)
    s = expr_str.strip()
    if not s:
        return F.lit(None)
    # Explicit literal from code parser
    # (code parser may set "literal": true and "value": 16)
    # Handled by caller passing e.get("value") when literal is True
    # Numeric literal: use F.lit so we don't try to resolve column "16"
    if s.lstrip("-").isdigit():
        return F.lit(int(s))
    try:
        if "." in s and s.lstrip("-").replace(".", "", 1).isdigit():
            return F.lit(float(s))
    except ValueError:
        pass
    # Quoted string literal: 'x' or "x"
    if (len(s) >= 2 and s[0] == s[-1] and s[0] in ("'", '"')):
        return F.lit(s[1:-1])
    # Column reference
    src = _col(s)
    resolved = _resolve_col(df, src)
    if resolved in df.columns:
        return F.col(resolved)
    # Fallback: still a literal (e.g. alphanumeric code like "OK"); use as string
    return F.lit(s)


def _parse_condition(cond: str, df: DataFrame):
    """Parse a condition string like \"TXN_TYPE = 'DR'\" into a Spark column expression."""
    if not cond or not isinstance(cond, str):
        return None
    cond = cond.strip()
    m = re.match(r"(\w+)\s*=\s*['\"]([^'\"]*)['\"]", cond, re.IGNORECASE)
    if m:
        col_name = _resolve_col(df, m.group(1))
        val = m.group(2)
        return F.col(col_name) == val
    return None


def apply_transformation_step(
    step: dict,
    datasets: dict[str, "DataFrame"],
) -> tuple[str | None, DataFrame | None]:
    """
    Apply one transformation step: resolve source dataset(s) from the registry,
    run a single Spark DataFrame transformation (filter, join, aggregate, select, etc.),
    return (output_alias, result_df). The caller stores the result in the registry
    so the next step can use it. No Spark SQL; uses only DataFrame API.
    """
    executor = MainframeTransformationExecutor(datasets)
    return executor.apply_step(step)


class MainframeTransformationExecutor:
    """
    Applies one step at a time: reads source DataFrame(s) from the registry,
    runs the Spark transformation for this step type, returns the result.
    """

    def __init__(self, dfs: dict[str, DataFrame]):
        """
        Args:
            dfs: Registry of named DataFrames (inputs + outputs of previous steps).
                  This dict is updated in place when a step produces an output (same dict as runner's datasets).
        """
        self.dfs = dfs  # use same dict so step output is visible to next step

    def apply_step(self, step: dict) -> tuple[str | None, DataFrame | None]:
        """
        Apply a single transformation step.

        Args:
            step: Dict with keys: id, type, source_inputs, logic, output_alias.

        Returns:
            (output_alias, result_df) or (None, None) on error.
        """
        step_type = (step.get("type") or "select").lower()
        logic = step.get("logic") or {}
        source_names = step.get("source_inputs") or []
        alias = step.get("output_alias") or step.get("id", "out")

        # Resolve source dataframe(s)
        source_df: DataFrame | None = None
        for name in source_names:
            if name in self.dfs:
                source_df = self.dfs[name]
                break
        if source_df is None:
            LOG.warning("No source found for step %s, sources: %s", step.get("id"), source_names)
            return None, None

        result: DataFrame | None = None

        if step_type == "filter":
            result = self._apply_filter(source_df, logic)
        elif step_type == "join":
            result = self._apply_join(logic)
        elif step_type == "aggregate":
            result = self._apply_aggregate(source_df, logic)
        elif step_type == "union":
            result = self._apply_union(logic, step)
        elif step_type == "custom":
            op = (logic.get("operation") or logic.get("op") or "").lower()
            if op == "sort":
                result = self._apply_sort(source_df, logic)
            elif op == "merge":
                result = self._apply_merge(logic, step)
            else:
                result = source_df
        elif step_type == "select":
            result = self._apply_select(source_df, logic)
        else:
            result = source_df

        if result is not None:
            self.dfs[alias] = result
            return alias, result
        return None, None

    def _apply_filter(self, df: DataFrame, logic: dict) -> DataFrame:
        conditions = logic.get("conditions") or []
        # Support else_branch: filter where condition is False (invalid path → output_alias)
        if logic.get("else_branch") and isinstance(logic.get("value_list"), list):
            # Invalid path: keep rows where field NOT IN value_list
            field = _col(logic.get("field", ""))
            val_list = logic["value_list"]
            col_ref = F.col(_resolve_col(df, field))
            return df.filter(~col_ref.isin(val_list))
        if not conditions:
            return df
        cond_expr = None
        for c in conditions:
            field = _col(c.get("field", ""))
            op = (c.get("operation") or c.get("op") or "==")
            op = str(op).strip().lower()
            val = c.get("value")
            col_ref = F.col(_resolve_col(df, field))
            if op in ("in", "in_list"):
                vals = val if isinstance(val, (list, tuple)) else [val]
                expr = col_ref.isin(vals)
            elif op in ("not_in", "not_in_list", "nin"):
                vals = val if isinstance(val, (list, tuple)) else [val]
                expr = ~col_ref.isin(vals)
            elif op in (">", "gt", "greater", "greater_than"):
                expr = col_ref > val
            elif op in ("<", "lt", "less", "less_than"):
                expr = col_ref < val
            elif op in (">=", "ge", "greater_equal"):
                expr = col_ref >= val
            elif op in ("<=", "le", "less_equal"):
                expr = col_ref <= val
            elif op in ("!=", "<>", "ne", "not_equal"):
                expr = col_ref != val
            else:
                expr = col_ref == val
            cond_expr = expr if cond_expr is None else (cond_expr & expr)
        if cond_expr is not None:
            return df.filter(cond_expr)
        return df

    def _apply_join(self, logic: dict) -> DataFrame | None:
        left_name = logic.get("left")
        right_name = logic.get("right")
        on_spec = logic.get("on") or []
        how = (logic.get("how") or "inner").lower()

        left_df = self.dfs.get(left_name) if left_name else None
        right_df = self.dfs.get(right_name) if right_name else None
        if left_df is None or right_df is None:
            return None

        join_expr = None
        for pair in on_spec:
            if isinstance(pair, (list, tuple)) and len(pair) >= 2:
                lc = _col(str(pair[0]))
                rc = _col(str(pair[1]))
                ex = F.col(_resolve_col(left_df, lc)) == F.col(_resolve_col(right_df, rc))
                join_expr = ex if join_expr is None else (join_expr & ex)
            elif isinstance(pair, str):
                c = _col(pair)
                ex = F.col(_resolve_col(left_df, c)) == F.col(_resolve_col(right_df, c))
                join_expr = ex if join_expr is None else (join_expr & ex)
        if join_expr is None:
            return None
        return left_df.join(right_df, join_expr, how)

    def _apply_aggregate(self, df: DataFrame, logic: dict) -> DataFrame:
        group_cols = logic.get("group_by") or []
        aggs = logic.get("aggregations") or []

        g_cols = [F.col(_resolve_col(df, _col(x))) for x in group_cols]
        agg_exprs = []
        for a in aggs:
            f = a.get("field", "*")
            op = (a.get("operation") or a.get("op") or "sum").lower()
            alias = a.get("alias") or f"{op}_{_col(f)}"
            col_ref = F.col(_resolve_col(df, _col(f))) if f != "*" else None
            cond = a.get("condition")
            cond_expr = _parse_condition(cond, df) if cond else None
            if op in ("sum", "add"):
                if f != "*":
                    if cond_expr is not None:
                        agg_exprs.append(F.sum(F.when(cond_expr, col_ref).otherwise(0)).alias(alias))
                    else:
                        agg_exprs.append(F.sum(col_ref).alias(alias))
            elif op in ("count", "tallying"):
                if cond_expr is not None:
                    agg_exprs.append(F.sum(F.when(cond_expr, 1).otherwise(0)).alias(alias))
                else:
                    agg_exprs.append(F.count(F.lit(1) if f == "*" else col_ref).alias(alias))
            elif op == "avg":
                if cond_expr is not None:
                    agg_exprs.append(F.avg(F.when(cond_expr, col_ref).otherwise(None)).alias(alias))
                else:
                    agg_exprs.append(F.avg(col_ref).alias(alias))
            elif op == "min":
                agg_exprs.append(F.min(col_ref).alias(alias))
            elif op == "max":
                agg_exprs.append(F.max(col_ref).alias(alias))
            else:
                if cond_expr is not None and col_ref is not None:
                    agg_exprs.append(F.sum(F.when(cond_expr, col_ref).otherwise(0)).alias(alias))
                else:
                    agg_exprs.append(F.sum(col_ref).alias(alias))
        if not agg_exprs:
            return df
        if g_cols:
            return df.groupBy(*g_cols).agg(*agg_exprs)
        return df.agg(*agg_exprs)

    def _apply_union(self, logic: dict, step: dict | None = None) -> DataFrame | None:
        source_names = logic.get("source_inputs") or (step.get("source_inputs") if step else [])
        frames = [self.dfs[n] for n in source_names if n in self.dfs]
        if len(frames) < 2:
            return frames[0] if frames else None
        return frames[0].unionByName(frames[1], allowMissingColumns=True)

    def _apply_sort(self, df: DataFrame, logic: dict) -> DataFrame:
        key = logic.get("key") or logic.get("keys", [])
        asc = (logic.get("ascending", True) if "ascending" in logic
               else "desc" not in (logic.get("order") or "asc").lower())
        if isinstance(key, str):
            key = [key]
        cols = [F.col(_resolve_col(df, _col(k))) for k in key]
        return df.orderBy(*cols, ascending=asc)

    def _apply_merge(self, logic: dict, step: dict | None = None) -> DataFrame | None:
        return self._apply_union(logic, step)

    def _apply_select(self, df: DataFrame, logic: dict) -> DataFrame:
        expressions = logic.get("expressions") or []
        columns = logic.get("columns")

        if columns:
            if columns == ["*"]:
                return df
            cols = [F.col(_resolve_col(df, _col(c))).alias(_col(c)) for c in columns]
            return df.select(*cols) if cols else df

        result = df
        for e in expressions:
            target = _col(e.get("target", ""))
            expr_str = e.get("expression", "")
            op = (e.get("operation") or e.get("op") or "move").lower()

            if op == "move":
                if e.get("literal") is True:
                    result = result.withColumn(target, F.lit(e.get("value", expr_str)))
                else:
                    result = result.withColumn(target, _expression_to_column(result, expr_str, op))

            elif op == "add":
                # Working storage: if target doesn't exist (e.g. WS_DEBIT_TOTAL), start from 0
                parts = expr_str.replace("+", " ").split()
                val_cols = [_resolve_col(result, _col(p)) for p in parts if p != target]
                val_consts = [int(p) for p in parts if p.lstrip("-").isdigit()]
                if _column_exists(result, target):
                    base = _resolve_col(result, target)
                    acc = F.col(base)
                else:
                    acc = F.lit(0)
                for v in val_cols:
                    if v in result.columns:
                        acc = acc + F.col(v)
                for v in val_consts:
                    acc = acc + F.lit(v)
                result = result.withColumn(target, acc)

            elif op == "subtract":
                src = _col(expr_str.strip().split()[0] if expr_str else "")
                src_resolved = _resolve_col(result, src)
                if _column_exists(result, target):
                    base = _resolve_col(result, target)
                    base_val = F.col(base)
                else:
                    base_val = F.lit(0)
                if src_resolved in result.columns:
                    result = result.withColumn(target, base_val - F.col(src_resolved))
                else:
                    result = result.withColumn(target, base_val)

            elif op == "multiply":
                parts = expr_str.replace("*", " ").split()
                other = next((_resolve_col(result, _col(p)) for p in parts if p != target), None)
                if _column_exists(result, target):
                    base = _resolve_col(result, target)
                    base_val = F.col(base)
                else:
                    base_val = F.lit(1)
                if other and other in result.columns:
                    result = result.withColumn(target, base_val * F.col(other))

            elif op == "divide":
                src = _col(expr_str.strip().split()[0] if expr_str else "")
                src_resolved = _resolve_col(result, src)
                if _column_exists(result, target):
                    base = _resolve_col(result, target)
                    base_val = F.col(base)
                else:
                    base_val = F.lit(0)
                if src_resolved in result.columns:
                    result = result.withColumn(
                        target,
                        F.when(F.col(src_resolved) != 0, base_val / F.col(src_resolved))
                        .otherwise(F.lit(None)),
                    )
                else:
                    result = result.withColumn(target, F.lit(None))

            elif op == "compute":
                # Normalize: COBOL often uses _ for minus in expressions; Spark expr needs -
                safe_expr = expr_str.replace("-", "_")
                safe_expr = re.sub(r"\s+_\s+", " - ", safe_expr)
                try:
                    result = result.withColumn(target, F.expr(safe_expr))
                except Exception:
                    result = result.withColumn(target, F.lit(expr_str))

            elif op == "initialize":
                default = e.get("value", 0)
                result = result.withColumn(target, F.lit(default))

            elif op == "string":
                delim = e.get("delimiter", "")
                parts = [p.strip() for p in expr_str.split() if p.strip() and p.upper() not in ("BY", "DELIMITED", "SIZE")]
                cols = [F.col(_resolve_col(result, _col(p))) for p in parts if p]
                if cols:
                    result = result.withColumn(target, F.concat_ws(delim, *cols))

            elif op == "unstring":
                delim = e.get("delimiter", ",")
                src = _col(expr_str.strip().split()[0] if expr_str else "")
                result = result.withColumn(target, F.split(F.col(_resolve_col(result, src)), delim).getItem(0))

            elif op == "inspect":
                src = _col(expr_str.strip().split()[0] if expr_str else "")
                before = e.get("before", "")
                after = e.get("after", "")
                result = result.withColumn(
                    target,
                    F.regexp_replace(F.col(_resolve_col(result, src)), F.lit(before), F.lit(after)),
                )

            else:
                try:
                    result = result.withColumn(target, F.expr(expr_str.replace("-", "_")))
                except Exception:
                    result = result.withColumn(target, F.lit(expr_str))

        return result
