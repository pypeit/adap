from datetime import datetime
from pathlib import Path

from extended_spec_mixins import get_lris_spec_name

exclude_pypeit_types = {"LRIS":     ["bias", "dark"],
                        "LRISBLUE": ["bias", "dark"],
                        }

spec_to_instrument = {"keck_lris_blue": "LRISBLUE",
                      "keck_lris_blue_orig": "LRISBLUE",
                      "keck_lris_red": "LRIS",
                      "keck_lris_red_orig": "LRIS",
                      "keck_lris_red_mark4": "LRIS",
                     }

def dataset_to_spec(dataset_name):
    """Return the name of the PypeIt spectrograph a dataset reduces with.

    A dataset on this branch is ``<target>/<YYYYMMDD>/<LRIS|LRISBLUE>``. The arm and the
    observation date together select the spectrograph, because the LRIS detectors were
    upgraded several times; ``get_lris_spec_name`` holds those dates.

    Args:
        dataset_name (str or :obj:`pathlib.Path`): The name of a dataset.

    Returns:
        str: The PypeIt spectrograph name, for example ``keck_lris_red_orig``.

    Raises:
        ValueError: If the name is not a full dataset. A dataset *prefix* does not name
            one arm of one night, so no single spectrograph corresponds to it.
    """
    parts = Path(dataset_name).parts
    if len(parts) != 3:
        raise ValueError(f"Can't get a spectrograph from '{dataset_name}'; "
                         "expected a dataset named <target>/<YYYYMMDD>/<instrument>.")

    target, date_str, instrument = parts

    if instrument not in spec_to_instrument.values():
        raise ValueError(f"Unrecognized instrument '{instrument}' in dataset "
                         f"'{dataset_name}'; expected one of "
                         f"{sorted(set(spec_to_instrument.values()))}.")

    try:
        obs_date = datetime.strptime(date_str, "%Y%m%d").date()
    except ValueError:
        raise ValueError(f"Can't parse an observation date from '{date_str}' in dataset "
                         f"'{dataset_name}'; expected YYYYMMDD.") from None

    return get_lris_spec_name(obs_date=obs_date, instrument=instrument)
