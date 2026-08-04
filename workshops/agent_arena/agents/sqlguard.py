import re

_FORBIDDEN = re.compile(
    r"\b(INSERT|UPDATE|DELETE|DROP|ALTER|CREATE|TRUNCATE|RENAME|ATTACH|DETACH|"
    r"OPTIMIZE|GRANT|REVOKE|SET|SYSTEM|KILL|INTO\s+OUTFILE|INTO\s+DUMPFILE)\b",
    re.IGNORECASE,
)


def _strip_comments(sql: str) -> str:
    sql = re.sub(r"--[^\n]*", " ", sql)
    sql = re.sub(r"/\*.*?\*/", " ", sql, flags=re.DOTALL)
    return sql


def validate_select_only(sql: str) -> tuple[bool, str | None]:
    """Accept exactly one SELECT or WITH...SELECT statement. Reject everything else."""
    if sql is None:
        return False, "empty SQL"
    cleaned = _strip_comments(sql).strip().rstrip(";").strip()
    if not cleaned:
        return False, "empty SQL"
    if ";" in cleaned:
        return False, "multiple statements are not allowed"
    head = cleaned[:6].upper()
    if not (head.startswith("SELECT") or head.startswith("WITH")):
        return False, "only SELECT / WITH...SELECT statements are allowed"
    if _FORBIDDEN.search(cleaned):
        return False, "statement contains a forbidden keyword"
    cte_names = {
        match.group(1).lower()
        for match in re.finditer(r"(?:\bWITH\b|,)\s*([A-Za-z_][\w]*)\s+AS\s*\(",
                                 cleaned, flags=re.IGNORECASE)
    }
    for match in re.finditer(r"\b(?:FROM|JOIN)\s+([`\w.]+)", cleaned,
                             flags=re.IGNORECASE):
        identifier = match.group(1).replace("`", "")
        table = identifier.rsplit(".", 1)[-1].lower()
        if table not in cte_names and not table.startswith("v_"):
            return False, f"queries may reference only v_* views (found {identifier})"
    return True, None
