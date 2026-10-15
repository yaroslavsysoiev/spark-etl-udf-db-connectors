import pandas as pd
import pycountry # provides standardized information about countries

from pyspark.sql.functions import pandas_udf
from pyspark.sql.types import StringType


@pandas_udf(StringType())
def country_to_iso(countries: pd.Series) -> pd.Series:
    def convert(value):
        if pd.isna(value):
            return None

        result = []

        for country_name in value.split(","):
            country_name = country_name.strip()

            try:
                # find information about the country i.e. its ISO code
                country = pycountry.countries.lookup(country_name)

                # return 2-symbol country ISO code and add it to the result
                result.append(country.alpha_2)

            except LookupError:
                result.append("UNKNOWN")

        return ",".join(result) # Join all country codes into a single comma-separated string

    # execute convert func on each element of pd.Series
    return countries.apply(convert)
