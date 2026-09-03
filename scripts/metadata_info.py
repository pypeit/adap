from pathlib import Path

exclude_pypeit_types = {"DEIMOS": ["bias"],
                        "ESI":     ["standard"],
                        "HIRES":   ["bias", "dark"],
                        "LRIS":    ["bias", "dark"],
                        "LRISBLUE":    ["bias", "dark"],
                        "MOSFIRE": [],
                        "NIRES":   [],
                        "NIRSPEC": [],
                        }

spec_to_instrument = {"keck_deimos": "DEIMOS",
                      "keck_esi": "ESI",
                      "keck_hires": "HIRES",
                      "keck_lris_blue": "LRISBLUE",
                      "keck_lris_blue_orig": "LRISBLUE",
                      "keck_lris_red": "LRIS",
                      "keck_lris_red_orig": "LRIS",
                      "keck_lris_red_mark4": "LRIS",
                      "keck_mosfire": "MOSFIRE",
                      "keck_nires": "NIRES",
                      "keck_nirspec": "NIRSPEC",
                     }

def dataset_to_spec(dataset_name):
    dataset_path = Path(dataset_name)
    instrument = dataset_path.parts[0]
    if instrument == "LRIS":
        config_name = dataset_path.parts[2]
        split_config = config_name.split('_')
        if split_config[0] != 'keck' or split_config[1] != 'lris':
            raise ValueError(f"Can't parse LRIS config name {config_name}")
        if split_config[2] == 'red':
            if split_config[3] == "orig":
                return "keck_lris_red_orig"
            elif split_config[3] == "mark4":
                return "keck_lris_red_mark4"
            else:
                return "keck_lris_red"
        elif split_config[2] == "blue":
            if split_config[3] == "orig":
                return "keck_lris_blue_orig"
            else:
                return "keck_lris_blue"
        else:
            raise ValueError(f"Can't parse LRIS config name {config_name}")
    else:
        return f"keck_{instrument.lower()}"
