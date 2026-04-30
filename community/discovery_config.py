"""
discovery_config.py — Shared configuration for Meetup and Luma discovery scripts.

Centralizes topics/keywords and regions so both platforms use the same coverage.
"""

# ---------------------------------------------------------------------------
# Topics / Keywords
# ---------------------------------------------------------------------------
# Used by Meetup (as search keywords) and Luma (as category slugs where available)

TOPICS = [
    # Programming languages (consolidated)
    "python",
    "rust",
    "golang",
    "javascript",
    "typescript",
    "java",
    "kotlin",
    "swift",
    "ruby",
    "elixir",
    "scala",
    "haskell",
    "functional programming",
    "c++",
    "dotnet",
    "C#",
    "php",
    "django",
    "rails",

    # Data & AI (high-signal terms)
    "data science",
    "machine learning",
    "artificial intelligence",
    "generative AI",
    "LLM",
    "deep learning",
    "NLP",
    "computer vision",
    "data engineering",
    "MLOps",
    "pytorch",
    "tensorflow",
    "spark",
    "airflow",
    "dbt",

    # Infrastructure & DevOps (consolidated)
    "kafka",
    "kubernetes",
    "docker",
    "devops",
    "cloud native",
    "terraform",
    "AWS",
    "azure",
    "GCP",
    "platform engineering",
    "SRE",
    "observability",
    "CI/CD",
    "serverless",
    "gitops",

    # Web & Frontend (consolidated)
    "react",
    "vue",
    "angular",
    "svelte",
    "next.js",
    "frontend",
    "backend",
    "fullstack",
    "web development",
    "node.js",
    "graphql",
    "API",
    "microservices",

    # Databases (consolidated)
    "postgresql",
    "mongodb",
    "redis",
    "elasticsearch",
    "database",
    "SQL",
    "neo4j",
    "vector database",

    # Security
    "security",
    "cybersecurity",
    "devsecops",
    "ethical hacking",
    "OWASP",

    # General tech (high-signal)
    "open source",
    "developer",
    "tech meetup",
    "software engineering",
    "software architecture",
    "distributed systems",
    "agile",
    "testing",
    "startup",
    "product management",
    "UX",
    "hackathon",
    "women in tech",

    # Blockchain & Web3 (consolidated)
    "crypto",
    "blockchain",
    "web3",
    "ethereum",
    "solidity",

    # Mobile (consolidated)
    "mobile development",
    "iOS",
    "android",
    "flutter",
    "react native",

    # Gaming & XR (consolidated)
    "game development",
    "unity",
    "unreal engine",
    "VR",
    "AR",

    # Hardware & IoT (consolidated)
    "IoT",
    "embedded",
    "robotics",
    "raspberry pi",
    "arduino",
    "maker",

    # Emerging tech (consolidated)
    "quantum computing",
    "biotech",
    "climate tech",
    "fintech",
    "healthtech",
]

# Luma-specific category slugs (pages that exist on lu.ma/<slug>)
# These map loosely to TOPICS but use Luma's specific URL structure
LUMA_CATEGORIES = [
    "tech",
    "ai",
    "science",
    "design",
    "climate",
    "music",
    "sports",
    "finance",
    "crypto",
    "gaming",
    "wellness",
    "food",
    "travel",
    # City-specific AI pages
    "genai-sf",
    "genai-nyc",
    # "genai-london",  # 404s
]


# ---------------------------------------------------------------------------
# Regions / Cities
# ---------------------------------------------------------------------------
# (label, lat, lon, radius_miles, luma_slug)
# - lat/lon/radius used by Meetup for geo search
# - luma_slug used by Luma for city page scraping (lu.ma/<slug>)

REGIONS = [
    # UK & Ireland
    ("London",          51.51,    -0.12,  30, "london"),
    ("Manchester",      53.48,    -2.24,  30, "manchester"),
    ("Edinburgh",       55.95,    -3.19,  30, "edinburgh"),
    ("Bristol",         51.45,    -2.59,  30, "bristol"),
    ("Birmingham",      52.48,    -1.90,  30, None),
    ("Leeds",           53.80,    -1.55,  30, None),
    ("Glasgow",         55.86,    -4.25,  30, None),
    ("Cambridge",       52.21,     0.12,  20, None),
    ("Oxford",          51.75,    -1.25,  20, None),
    ("Newcastle",       54.98,    -1.61,  25, None),
    ("Sheffield",       53.38,    -1.47,  25, None),
    ("Liverpool",       53.41,    -2.98,  25, None),
    ("Nottingham",      52.95,    -1.15,  25, None),
    ("Southampton",     50.91,    -1.40,  25, None),
    ("Reading",         51.45,    -0.97,  20, None),
    ("Dublin",          53.33,    -6.25,  30, "dublin"),
    ("Belfast",         54.60,    -5.93,  25, None),
    ("Cork",            51.90,    -8.47,  25, None),
    ("Galway",          53.27,    -9.06,  20, None),

    # Western Europe - Germany
    ("Berlin",          52.52,    13.40,  30, "berlin"),
    ("Hamburg",         53.55,     9.99,  30, None),
    ("Frankfurt",       50.11,     8.68,  30, None),
    ("Cologne",         50.94,     6.96,  25, None),
    ("Munich",          48.14,    11.58,  30, "munich"),
    ("Stuttgart",       48.78,     9.18,  25, None),
    ("Dusseldorf",      51.23,     6.78,  25, None),
    ("Leipzig",         51.34,    12.37,  25, None),
    ("Dresden",         51.05,    13.74,  25, None),
    ("Hannover",        52.37,     9.74,  25, None),
    ("Nuremberg",       49.45,    11.08,  25, None),

    # Western Europe - Benelux
    ("Amsterdam",       52.37,     4.90,  30, "amsterdam"),
    ("Rotterdam",       51.92,     4.48,  25, None),
    ("The Hague",       52.08,     4.31,  20, None),
    ("Utrecht",         52.09,     5.12,  20, None),
    ("Eindhoven",       51.44,     5.47,  20, None),
    ("Brussels",        50.85,     4.35,  30, "brussels"),
    ("Antwerp",         51.22,     4.40,  25, None),
    ("Ghent",           51.05,     3.73,  20, None),
    ("Luxembourg",      49.61,     6.13,  20, None),

    # Western Europe - France
    ("Paris",           48.86,     2.35,  30, "paris"),
    ("Lyon",            45.76,     4.84,  25, None),
    ("Marseille",       43.30,     5.37,  25, None),
    ("Toulouse",        43.60,     1.44,  25, None),
    ("Nice",            43.71,     7.26,  20, None),
    ("Nantes",          47.22,    -1.55,  25, None),
    ("Bordeaux",        44.84,    -0.58,  25, None),
    ("Lille",           50.63,     3.06,  25, None),
    ("Strasbourg",      48.57,     7.75,  25, None),
    ("Montpellier",     43.61,     3.88,  20, None),

    # Iberia
    ("Barcelona",       41.39,     2.16,  30, "barcelona"),
    ("Madrid",          40.42,    -3.70,  30, "madrid"),
    ("Valencia",        39.47,    -0.38,  25, None),
    ("Seville",         37.39,    -5.98,  25, None),
    ("Bilbao",          43.26,    -2.93,  20, None),
    ("Malaga",          36.72,    -4.42,  20, None),
    ("Lisbon",          38.72,    -9.14,  30, "lisbon"),
    ("Porto",           41.16,    -8.63,  25, None),
    ("Braga",           41.55,    -8.43,  20, None),

    # Nordics
    ("Stockholm",       59.33,    18.07,  30, "stockholm"),
    ("Gothenburg",      57.71,    11.97,  25, None),
    ("Malmo",           55.60,    13.00,  20, None),
    ("Copenhagen",      55.68,    12.57,  30, "copenhagen"),
    ("Aarhus",          56.16,    10.20,  20, None),
    ("Oslo",            59.91,    10.75,  30, "oslo"),
    ("Bergen",          60.39,     5.32,  20, None),
    ("Trondheim",       63.43,    10.40,  20, None),
    ("Helsinki",        60.17,    24.94,  30, "helsinki"),
    ("Tampere",         61.50,    23.79,  20, None),
    ("Turku",           60.45,    22.27,  20, None),
    ("Reykjavik",       64.15,   -21.94,  25, None),

    # Central Europe - DACH
    ("Zurich",          47.38,     8.54,  30, "zurich"),
    ("Geneva",          46.20,     6.14,  25, None),
    ("Basel",           47.56,     7.59,  20, None),
    ("Bern",            46.95,     7.45,  20, None),
    ("Lausanne",        46.52,     6.63,  20, None),
    ("Vienna",          48.21,    16.37,  30, "vienna"),
    ("Graz",            47.07,    15.44,  20, None),
    ("Salzburg",        47.80,    13.04,  20, None),
    ("Linz",            48.31,    14.29,  20, None),

    # Central Europe - Eastern
    ("Warsaw",          52.23,    21.01,  30, "warsaw"),
    ("Krakow",          50.06,    19.94,  25, None),
    ("Wroclaw",         51.11,    17.04,  25, None),
    ("Poznan",          52.41,    16.93,  20, None),
    ("Gdansk",          54.35,    18.65,  20, None),
    ("Lodz",            51.76,    19.46,  20, None),
    ("Prague",          50.08,    14.44,  30, "prague"),
    ("Brno",            49.20,    16.61,  20, None),
    ("Budapest",        47.50,    19.04,  30, None),
    ("Bucharest",       44.43,    26.10,  30, None),
    ("Cluj-Napoca",     46.77,    23.60,  20, None),
    ("Timisoara",       45.76,    21.23,  20, None),

    # Southern Europe - Italy
    ("Milan",           45.46,     9.19,  30, "milan"),
    ("Rome",            41.90,    12.50,  30, None),
    ("Turin",           45.07,     7.69,  25, None),
    ("Florence",        43.77,    11.25,  25, None),
    ("Bologna",         44.49,    11.34,  20, None),
    ("Naples",          40.85,    14.27,  25, None),
    ("Venice",          45.44,    12.32,  20, None),
    ("Padua",           45.41,    11.88,  20, None),

    # Southern Europe - Greece/Cyprus
    ("Athens",          37.98,    23.73,  30, None),
    ("Thessaloniki",    40.64,    22.94,  25, None),
    ("Nicosia",         35.19,    33.38,  20, None),

    # Balkans
    ("Zagreb",          45.81,    15.98,  25, None),
    ("Belgrade",        44.82,    20.46,  25, None),
    ("Sofia",           42.70,    23.32,  25, None),
    ("Ljubljana",       46.05,    14.51,  20, None),
    ("Sarajevo",        43.86,    18.41,  20, None),
    ("Skopje",          42.00,    21.43,  20, None),
    ("Tirana",          41.33,    19.82,  20, None),

    # Baltics
    ("Tallinn",         59.44,    24.75,  25, None),
    ("Tartu",           58.38,    26.72,  15, None),
    ("Riga",            56.95,    24.11,  25, None),
    ("Vilnius",         54.69,    25.28,  25, None),
    ("Kaunas",          54.90,    23.90,  20, None),

    # US - West Coast
    ("San Francisco",   37.77,  -122.42,  30, "sf"),
    ("Oakland",         37.80,  -122.27,  20, None),
    ("San Jose",        37.34,  -121.89,  25, None),
    ("Palo Alto",       37.44,  -122.14,  15, None),
    ("Mountain View",   37.39,  -122.08,  15, None),
    ("Los Angeles",     34.05,  -118.24,  30, "la"),
    ("Santa Monica",    34.02,  -118.49,  15, None),
    ("Pasadena",        34.15,  -118.14,  15, None),
    ("Irvine",          33.68,  -117.83,  20, None),
    ("San Diego",       32.72,  -117.16,  25, None),
    ("Seattle",         47.61,  -122.33,  30, "seattle"),
    ("Bellevue",        47.61,  -122.20,  15, None),
    ("Portland",        45.52,  -122.68,  25, None),
    ("Sacramento",      38.58,  -121.49,  25, None),
    ("Phoenix",         33.45,  -112.07,  30, None),
    ("Tucson",          32.22,  -110.93,  20, None),
    ("Las Vegas",       36.17,  -115.14,  25, None),
    ("Salt Lake City",  40.76,  -111.89,  25, None),
    ("Honolulu",        21.31,  -157.86,  20, None),

    # US - Mountain & Central
    ("Denver",          39.74,  -104.98,  30, "denver"),
    ("Boulder",         40.01,  -105.27,  20, None),
    ("Colorado Springs",38.83,  -104.82,  20, None),
    ("Austin",          30.27,   -97.74,  30, "austin"),
    ("Dallas",          32.78,   -96.80,  30, None),
    ("Fort Worth",      32.76,   -97.33,  20, None),
    ("Houston",         29.76,   -95.37,  30, None),
    ("San Antonio",     29.42,   -98.49,  25, None),
    ("Chicago",         41.88,   -87.63,  30, "chicago"),
    ("Minneapolis",     44.98,   -93.27,  25, None),
    ("St Paul",         44.95,   -93.09,  15, None),
    ("Milwaukee",       43.04,   -87.91,  20, None),
    ("Madison",         43.07,   -89.40,  20, None),
    ("Indianapolis",    39.77,   -86.16,  25, None),
    ("Columbus",        39.96,   -83.00,  25, None),
    ("Cleveland",       41.50,   -81.69,  25, None),
    ("Cincinnati",      39.10,   -84.51,  25, None),
    ("Detroit",         42.33,   -83.05,  25, None),
    ("Ann Arbor",       42.28,   -83.74,  15, None),
    ("St Louis",        38.63,   -90.20,  25, None),
    ("Kansas City",     39.10,   -94.58,  25, None),
    ("Nashville",       36.16,   -86.78,  25, None),
    ("New Orleans",     29.95,   -90.07,  25, None),
    ("Oklahoma City",   35.47,   -97.52,  25, None),

    # US - East Coast
    ("New York",        40.71,   -74.01,  30, "nyc"),
    ("Brooklyn",        40.65,   -73.95,  20, None),
    ("Jersey City",     40.72,   -74.05,  15, None),
    ("Boston",          42.36,   -71.06,  30, "boston"),
    ("Cambridge MA",    42.37,   -71.11,  15, None),
    ("Philadelphia",    39.95,   -75.17,  25, None),
    ("Washington DC",   38.91,   -77.04,  30, None),
    ("Arlington VA",    38.88,   -77.10,  15, None),
    ("Baltimore",       39.29,   -76.61,  25, None),
    ("Pittsburgh",      40.44,   -79.99,  25, None),
    ("Raleigh",         35.78,   -78.64,  30, None),
    ("Durham",          35.99,   -78.90,  15, None),
    ("Charlotte",       35.23,   -80.84,  25, None),
    ("Atlanta",         33.75,   -84.39,  30, None),
    ("Tampa",           27.95,   -82.46,  25, None),
    ("Orlando",         28.54,   -81.38,  25, None),
    ("Miami",           25.76,   -80.19,  30, None),
    ("Fort Lauderdale", 26.12,   -80.14,  20, None),
    ("Jacksonville",    30.33,   -81.66,  25, None),
    ("Providence",      41.82,   -71.41,  20, None),
    ("Hartford",        41.76,   -72.69,  20, None),
    ("New Haven",       41.31,   -72.92,  15, None),
    ("Buffalo",         42.89,   -78.88,  20, None),
    ("Rochester NY",    43.16,   -77.61,  20, None),
    ("Albany",          42.65,   -73.76,  20, None),

    # Canada
    ("Toronto",         43.65,   -79.38,  30, "toronto"),
    ("Mississauga",     43.59,   -79.64,  20, None),
    ("Vancouver",       49.25,  -123.12,  30, "vancouver"),
    ("Victoria",        48.43,  -123.37,  20, None),
    ("Montreal",        45.51,   -73.55,  30, None),
    ("Quebec City",     46.81,   -71.21,  20, None),
    ("Calgary",         51.05,  -114.07,  25, None),
    ("Edmonton",        53.55,  -113.49,  25, None),
    ("Ottawa",          45.42,   -75.70,  25, None),
    ("Waterloo",        43.46,   -80.52,  20, None),
    ("Kitchener",       43.45,   -80.49,  15, None),
    ("Hamilton",        43.26,   -79.87,  20, None),
    ("Winnipeg",        49.90,   -97.14,  25, None),
    ("Halifax",         44.65,   -63.58,  20, None),

    # Mexico
    ("Mexico City",     19.43,   -99.13,  30, None),
    ("Guadalajara",     20.67,  -103.35,  25, None),
    ("Monterrey",       25.67,  -100.31,  25, None),
    ("Tijuana",         32.51,  -117.03,  20, None),
    ("Puebla",          19.04,   -98.20,  20, None),
    ("Queretaro",       20.59,  -100.39,  20, None),

    # Central America & Caribbean
    ("San Jose CR",      9.93,   -84.08,  25, None),
    ("Panama City",      9.00,   -79.50,  25, None),
    ("Guatemala City",  14.63,   -90.51,  25, None),
    ("San Juan",        18.47,   -66.11,  20, None),
    ("Santo Domingo",   18.49,   -69.93,  25, None),

    # South America
    ("Sao Paulo",      -23.55,   -46.63,  30, "sao-paulo"),
    ("Rio de Janeiro", -22.91,   -43.17,  25, None),
    ("Belo Horizonte", -19.92,   -43.94,  25, None),
    ("Brasilia",       -15.79,   -47.88,  25, None),
    ("Curitiba",       -25.43,   -49.27,  25, None),
    ("Porto Alegre",   -30.03,   -51.23,  25, None),
    ("Recife",          -8.05,   -34.88,  25, None),
    ("Florianopolis",  -27.60,   -48.55,  20, None),
    ("Buenos Aires",   -34.60,   -58.38,  30, "buenos-aires"),
    ("Cordoba AR",     -31.42,   -64.18,  25, None),
    ("Rosario",        -32.95,   -60.65,  20, None),
    ("Mendoza",        -32.89,   -68.83,  20, None),
    ("Bogota",           4.71,   -74.07,  30, None),
    ("Medellin",         6.25,   -75.56,  25, None),
    ("Cali",             3.45,   -76.53,  25, None),
    ("Barranquilla",    10.96,   -74.80,  20, None),
    ("Lima",           -12.05,   -77.04,  30, None),
    ("Santiago",       -33.45,   -70.67,  30, None),
    ("Valparaiso",     -33.05,   -71.62,  20, None),
    ("Montevideo",     -34.90,   -56.16,  25, None),
    ("Quito",           -0.18,   -78.47,  25, None),
    ("Guayaquil",       -2.17,   -79.92,  25, None),
    ("Caracas",         10.48,   -66.90,  25, None),
    ("La Paz",         -16.50,   -68.15,  25, None),
    ("Asuncion",       -25.26,   -57.58,  25, None),

    # APAC - Japan
    ("Tokyo",           35.69,   139.69,  30, "tokyo"),
    ("Shibuya",         35.66,   139.70,  10, None),
    ("Osaka",           34.69,   135.50,  25, None),
    ("Kyoto",           35.01,   135.77,  20, None),
    ("Nagoya",          35.18,   136.91,  25, None),
    ("Fukuoka",         33.59,   130.40,  25, None),
    ("Sapporo",         43.06,   141.35,  25, None),
    ("Kobe",            34.69,   135.20,  20, None),
    ("Yokohama",        35.44,   139.64,  20, None),

    # APAC - Korea
    ("Seoul",           37.57,   126.98,  30, "seoul"),
    ("Gangnam",         37.50,   127.04,  10, None),
    ("Busan",           35.18,   129.08,  25, None),
    ("Incheon",         37.46,   126.71,  20, None),
    ("Daejeon",         36.35,   127.38,  20, None),
    ("Daegu",           35.87,   128.60,  20, None),

    # APAC - Greater China
    ("Taipei",          25.05,   121.53,  30, "taipei"),
    ("Hsinchu",         24.80,   120.97,  20, None),
    ("Taichung",        24.15,   120.67,  20, None),
    ("Hong Kong",       22.32,   114.17,  30, "hong-kong"),
    ("Beijing",         39.90,   116.41,  30, None),
    ("Shanghai",        31.23,   121.47,  30, None),
    ("Shenzhen",        22.54,   114.06,  30, None),
    ("Guangzhou",       23.13,   113.26,  30, None),
    ("Hangzhou",        30.27,   120.15,  25, None),
    ("Nanjing",         32.06,   118.80,  25, None),
    ("Chengdu",         30.57,   104.07,  25, None),
    ("Wuhan",           30.59,   114.31,  25, None),
    ("Xian",            34.27,   108.95,  25, None),
    ("Suzhou",          31.30,   120.62,  20, None),

    # APAC - Southeast Asia
    ("Singapore",        1.35,   103.82,  30, "singapore"),
    ("Bangkok",         13.76,   100.50,  30, None),
    ("Chiang Mai",      18.79,    98.98,  20, None),
    ("Kuala Lumpur",     3.14,   101.69,  30, None),
    ("Penang",           5.42,   100.33,  20, None),
    ("Johor Bahru",      1.49,   103.74,  20, None),
    ("Jakarta",         -6.21,   106.85,  30, None),
    ("Bandung",         -6.91,   107.61,  25, None),
    ("Surabaya",        -7.25,   112.75,  25, None),
    ("Bali",            -8.41,   115.19,  25, None),
    ("Ho Chi Minh",     10.82,   106.63,  30, None),
    ("Hanoi",           21.03,   105.85,  25, None),
    ("Da Nang",         16.05,   108.22,  20, None),
    ("Manila",          14.60,   120.98,  30, None),
    ("Makati",          14.55,   121.03,  15, None),
    ("Cebu",            10.32,   123.89,  20, None),
    ("Phnom Penh",      11.56,   104.93,  25, None),
    ("Yangon",          16.87,    96.20,  25, None),

    # APAC - South Asia
    ("Bangalore",       12.97,    77.59,  30, "bangalore"),
    ("Mumbai",          19.08,    72.88,  30, "mumbai"),
    ("Delhi",           28.70,    77.10,  30, None),
    ("Gurgaon",         28.46,    77.03,  20, None),
    ("Noida",           28.54,    77.39,  15, None),
    ("Hyderabad",       17.39,    78.49,  30, None),
    ("Chennai",         13.08,    80.27,  30, None),
    ("Pune",            18.52,    73.86,  25, None),
    ("Kolkata",         22.57,    88.36,  25, None),
    ("Ahmedabad",       23.02,    72.57,  25, None),
    ("Jaipur",          26.92,    75.79,  25, None),
    ("Kochi",            9.93,    76.27,  20, None),
    ("Thiruvananthapuram", 8.52,  76.94,  20, None),
    ("Chandigarh",      30.73,    76.78,  20, None),
    ("Indore",          22.72,    75.86,  20, None),
    ("Coimbatore",      11.02,    76.96,  20, None),
    ("Nagpur",          21.15,    79.09,  20, None),
    ("Lucknow",         26.85,    80.95,  25, None),
    ("Dhaka",           23.81,    90.41,  30, None),
    ("Colombo",          6.93,    79.85,  25, None),
    ("Karachi",         24.86,    67.01,  30, None),
    ("Lahore",          31.55,    74.34,  30, None),
    ("Islamabad",       33.69,    73.06,  25, None),
    ("Kathmandu",       27.72,    85.32,  25, None),

    # APAC - Oceania
    ("Sydney",         -33.87,   151.21,  30, "sydney"),
    ("North Sydney",   -33.84,   151.21,  10, None),
    ("Melbourne",      -37.81,   144.96,  30, "melbourne"),
    ("Brisbane",       -27.47,   153.03,  25, None),
    ("Gold Coast",     -28.02,   153.43,  20, None),
    ("Perth",          -31.95,   115.86,  25, None),
    ("Adelaide",       -34.93,   138.60,  25, None),
    ("Canberra",       -35.28,   149.13,  20, None),
    ("Hobart",         -42.88,   147.33,  20, None),
    ("Auckland",       -36.85,   174.76,  30, None),
    ("Wellington",     -41.29,   174.78,  25, None),
    ("Christchurch",   -43.53,   172.64,  20, None),

    # Middle East
    ("Tel Aviv",        32.08,    34.78,  30, "tel-aviv"),
    ("Jerusalem",       31.77,    35.23,  20, None),
    ("Haifa",           32.79,    34.99,  20, None),
    ("Dubai",           25.20,    55.27,  30, "dubai"),
    ("Abu Dhabi",       24.45,    54.37,  25, None),
    ("Sharjah",         25.36,    55.39,  15, None),
    ("Riyadh",          24.71,    46.68,  30, None),
    ("Jeddah",          21.54,    39.17,  25, None),
    ("Dammam",          26.43,    50.10,  20, None),
    ("Doha",            25.29,    51.53,  25, None),
    ("Kuwait City",     29.38,    47.99,  25, None),
    ("Manama",          26.23,    50.59,  20, None),
    ("Muscat",          23.61,    58.54,  25, None),
    ("Amman",           31.95,    35.93,  25, None),
    ("Beirut",          33.89,    35.50,  20, None),
    ("Istanbul",        41.01,    28.98,  30, None),
    ("Ankara",          39.93,    32.86,  25, None),
    ("Izmir",           38.42,    27.14,  25, None),
    ("Antalya",         36.90,    30.69,  20, None),
    ("Tehran",          35.70,    51.42,  30, None),

    # Africa - North
    ("Cairo",           30.04,    31.24,  30, None),
    ("Alexandria",      31.20,    29.92,  25, None),
    ("Casablanca",      33.59,    -7.62,  25, None),
    ("Rabat",           34.02,    -6.83,  20, None),
    ("Marrakech",       31.63,    -8.01,  20, None),
    ("Tunis",           36.81,    10.17,  25, None),
    ("Algiers",         36.75,     3.06,  25, None),

    # Africa - West
    ("Lagos",            6.52,     3.38,  30, "lagos"),
    ("Abuja",            9.08,     7.40,  25, None),
    ("Port Harcourt",    4.78,     7.01,  20, None),
    ("Ibadan",           7.38,     3.90,  20, None),
    ("Accra",            5.56,    -0.19,  25, None),
    ("Kumasi",           6.69,    -1.62,  20, None),
    ("Dakar",           14.69,   -17.44,  25, None),
    ("Abidjan",          5.36,    -4.01,  25, None),

    # Africa - East
    ("Nairobi",         -1.29,    36.82,  30, "nairobi"),
    ("Mombasa",         -4.05,    39.67,  20, None),
    ("Kigali",          -1.94,    30.06,  25, None),
    ("Kampala",          0.35,    32.58,  25, None),
    ("Dar es Salaam",   -6.79,    39.28,  25, None),
    ("Addis Ababa",      9.03,    38.75,  25, None),

    # Africa - South
    ("Cape Town",      -33.93,    18.42,  30, None),
    ("Johannesburg",   -26.20,    28.04,  30, None),
    ("Pretoria",       -25.75,    28.19,  20, None),
    ("Durban",         -29.86,    31.02,  25, None),
    ("Port Elizabeth", -33.96,    25.60,  20, None),
    ("Harare",         -17.83,    31.05,  25, None),
    ("Lusaka",         -15.39,    28.32,  25, None),
    ("Maputo",         -25.97,    32.57,  20, None),
    ("Gaborone",       -24.65,    25.91,  20, None),
    ("Windhoek",       -22.56,    17.08,  20, None),
    ("Mauritius",      -20.16,    57.50,  20, None),
]


# ---------------------------------------------------------------------------
# Helper functions
# ---------------------------------------------------------------------------

def get_meetup_cities() -> list[tuple[str, float, float, int]]:
    """Return (label, lat, lon, radius) tuples for Meetup geo search."""
    return [(r[0], r[1], r[2], r[3]) for r in REGIONS]


def get_luma_city_slugs() -> list[str]:
    """Return Luma city slugs (filtering out None)."""
    return [r[4] for r in REGIONS if r[4] is not None]


def get_meetup_keywords() -> list[str]:
    """Return keywords for Meetup search."""
    return TOPICS.copy()


def get_luma_categories() -> list[str]:
    """Return Luma category slugs."""
    return LUMA_CATEGORIES.copy()
