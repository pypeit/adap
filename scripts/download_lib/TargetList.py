"""
Sources of targets for the KOA download.

There are two of them. download.py takes a text file of "<name> <ra> <dec>" lines, which
is how a download is run by hand or locally. koa_download_from_queue.py takes rows from
the targets tab of the Scorecard spreadsheet, one target at a time, as they come off the
work queue. Both end up as Target objects fed to Query.query_position, so the step that
turns a name and a pair of coordinates into a Target belongs in one place rather than
being written twice.
"""

import Target


def make_target(name, ra, dec):
    """
    Build a Target from a name and a pair of coordinates in degrees.

    Raises ValueError if either coordinate is missing or is not a number. Both callers
    feed query_position, which formats the pair straight into a KOA cone search, so a
    blank or malformed coordinate would otherwise become a query for "circle None None"
    that fails inside KOA and comes back looking like a target with no data.
    """
    for label, value in (("ra", ra), ("dec", dec)):
        if value is None or str(value).strip() == '':
            raise ValueError(f'target {name} has no {label}')
        try:
            float(value)
        except (TypeError, ValueError):
            raise ValueError(f'target {name} has a non-numeric {label}: {value!r}')

    return Target.Target(name, str(ra).strip(), str(dec).strip())


def parse_target_file(filename):
    """
    Parse a target file: one "<name> <ra> <dec>" per line, ra and dec in degrees.
    Blank lines and lines starting with "#" are ignored.

    Every bad line is reported together, rather than stopping at the first, so that a
    malformed file can be corrected in one pass.
    """
    targets = []
    problems = []

    with open(filename, 'r', encoding='utf-8') as f:
        for lineno, line in enumerate(f, start=1):
            if line.startswith('#') or line.strip() == '':
                continue

            parts = line.split()
            name = parts[0]
            ra = parts[1] if len(parts) > 1 else None
            dec = parts[2] if len(parts) > 2 else None

            try:
                targets.append(make_target(name, ra, dec))
            except ValueError as e:
                problems.append(f'  line {lineno}: {e}')

    if len(problems) > 0:
        raise ValueError(f'{filename} has {len(problems)} unusable '
                         f'line(s):\n' + '\n'.join(problems))

    return targets


def parse_target_rows(rows):
    """
    Build Targets from rows read out of the targets tab.

    rows is a list of dicts with "name", "ra" and "dec" keys, as returned by
    gspread_utils.read_target_coords. Rows that cannot be turned into a Target are
    returned separately rather than raising, so that one bad row does not stop the
    others from being processed.

    Returns (targets, problems), where problems is a list of message strings.
    """
    targets = []
    problems = []

    for row in rows:
        try:
            targets.append(make_target(row.get('name'), row.get('ra'), row.get('dec')))
        except ValueError as e:
            problems.append(str(e))

    return targets, problems
