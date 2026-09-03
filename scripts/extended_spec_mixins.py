from datetime import date


def get_lris_spec_name(obs_date, koaid=None, instrument=None):
    if koaid is None and instrument is None:
        raise ValueError("Need koaid or instrument to identify Keck LRIS spectrograph subclass.")
    is_blue = False
    if instrument is not None:
        if instrument == "LRISBLUE":
            is_blue=True

    elif koaid is not None:
        if koaid.startswith("LB"):
            is_blue = True
        else:
            is_blue = False

    if is_blue:
        if obs_date <= date(2009,4,30):
            return "keck_lris_blue_orig"
        else:
            return "keck_lris_blue"
    else:
        if obs_date >= date(2021,4,22):
            return "keck_lris_red_mark4"
        elif obs_date <= date(2009,5,2):
            return "keck_lris_red_orig"
        else:
            return "keck_lris_red"
