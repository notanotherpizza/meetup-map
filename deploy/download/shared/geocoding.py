"""
shared/geocoding.py
───────────────────
Geocoding constants shared between the worker and any tooling.
The worker handles all geocoding logic directly using the Postgres
geocode_cache table — no local file cache, no stateful helpers.
"""

NOMINATIM_URL = "https://nominatim.openstreetmap.org/search"
NOMINATIM_DELAY = 1.5  # seconds between requests — OSM fair use policy

# Maps ISO 3166-1 alpha-2 country codes to full country names for
# unambiguous Nominatim queries (e.g. "Athens, Greece" not "Athens, gr"
# which could match Athens, Georgia in the US).
COUNTRY_CODE_TO_NAME = {
    "gb": "United Kingdom", "us": "United States", "de": "Germany",
    "fr": "France", "nl": "Netherlands", "br": "Brazil", "ng": "Nigeria",
    "za": "South Africa", "ke": "Kenya", "ug": "Uganda", "tz": "Tanzania",
    "gr": "Greece", "tr": "Turkey", "in": "India", "cn": "China",
    "jp": "Japan", "au": "Australia", "nz": "New Zealand", "ca": "Canada",
    "mx": "Mexico", "ar": "Argentina", "co": "Colombia", "cl": "Chile",
    "pe": "Peru", "ec": "Ecuador", "ph": "Philippines", "sg": "Singapore",
    "my": "Malaysia", "id": "Indonesia", "th": "Thailand", "vn": "Vietnam",
    "tw": "Taiwan", "hk": "Hong Kong", "pk": "Pakistan", "bd": "Bangladesh",
    "lk": "Sri Lanka", "np": "Nepal", "kw": "Kuwait", "ae": "United Arab Emirates",
    "sa": "Saudi Arabia", "il": "Israel", "eg": "Egypt", "ma": "Morocco",
    "gh": "Ghana", "et": "Ethiopia", "cm": "Cameroon", "sn": "Senegal",
    "ci": "Ivory Coast", "rw": "Rwanda", "tn": "Tunisia", "dz": "Algeria",
    "pl": "Poland", "cz": "Czech Republic", "sk": "Slovakia", "hu": "Hungary",
    "ro": "Romania", "bg": "Bulgaria", "hr": "Croatia", "rs": "Serbia",
    "si": "Slovenia", "mk": "North Macedonia", "ba": "Bosnia and Herzegovina",
    "ee": "Estonia", "lv": "Latvia", "lt": "Lithuania", "fi": "Finland",
    "se": "Sweden", "no": "Norway", "dk": "Denmark", "be": "Belgium",
    "at": "Austria", "ch": "Switzerland", "es": "Spain", "pt": "Portugal",
    "it": "Italy", "ie": "Ireland", "cy": "Cyprus", "mt": "Malta",
    "ru": "Russia", "ua": "Ukraine", "by": "Belarus", "md": "Moldova",
    "am": "Armenia", "ge": "Georgia", "az": "Azerbaijan", "kz": "Kazakhstan",
    "uz": "Uzbekistan", "mn": "Mongolia", "kr": "South Korea", "mm": "Myanmar",
    "kh": "Cambodia", "la": "Laos", "bn": "Brunei", "pg": "Papua New Guinea",
    "fj": "Fiji", "uy": "Uruguay", "py": "Paraguay", "bo": "Bolivia",
    "ve": "Venezuela", "cr": "Costa Rica", "pa": "Panama", "gt": "Guatemala",
    "hn": "Honduras", "ni": "Nicaragua", "sv": "El Salvador", "cu": "Cuba",
    "do": "Dominican Republic", "ht": "Haiti", "jm": "Jamaica",
    "tt": "Trinidad and Tobago", "bb": "Barbados", "gn": "Guinea",
    "ml": "Mali", "bf": "Burkina Faso", "ne": "Niger", "td": "Chad",
    "sd": "Sudan", "so": "Somalia", "mz": "Mozambique", "zm": "Zambia",
    "zw": "Zimbabwe", "bw": "Botswana", "na": "Namibia", "mg": "Madagascar",
    "mw": "Malawi", "ao": "Angola", "cd": "DR Congo", "ga": "Gabon",
    "ly": "Libya", "jo": "Jordan", "lb": "Lebanon", "iq": "Iraq",
    "ir": "Iran", "ye": "Yemen", "om": "Oman", "qa": "Qatar",
    "bh": "Bahrain", "ps": "Palestine", "mu": "Mauritius", "cv": "Cape Verde",
}