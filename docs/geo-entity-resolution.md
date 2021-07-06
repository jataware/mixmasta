
## mixmasta Geo Entity Resolution

## Example Cases
- [Latitude / Longitude](#latitude-/-longitude)
- [Coordinates](#coordinates)
- [Place Names](#place-names)

### Latitude / Longitude
If *latitude* and *longitude* are specified, *primary_geo* = *TRUE*, and *is_geo_pair*
refers to the paired value, then the lat/lon values are geocoded to [GADM](https://gadm.org/)
place names of country, admin1 (state/territory), admin2 (county/district), and if specified,
admin3 (municipality/town).

### Coordinates
If *coordinate* data is specified, then these values are translated to latitude and
longitude based on *coord_format*, and then geocoded to [GADM](https://gadm.org/) as above
for [Latitude / Longitude](#latitude-/-longitude).

### Place Names
If none of the above cases is satisifed, then specified geo fields
will be mapped to *country*, *admin1*, etc. Place names in these fields will be mapped
to [GADM](https://gadm.org/) place names *dependent on the assumption that the **country** is provided* and matches
the [GADM](https://gadm.org/) country name. Unmatched place names for admin1, admin2, and admin3, e.g. due to typos, misspellings, are mapped
to the best [GADM](https://gadm.org/) match using [Levenshtein distance](https://en.wikipedia.org/wiki/Levenshtein_distance) fuzzy string matching.

### ISO2 and ISO3 Coding
ISO2 and ISO3 columns identified as *iso2* and *iso3*, and marked as *primary_geo* = *TRUE*, will be converted to the corresponding
[GADM](https://gadm.org/) place name and reported in the *country* column.