import pandas as pd
import pycountry

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
                country = pycountry.countries.lookup(country_name)
                result.append(country.alpha_2)
            except LookupError:
                result.append("UNKNOWN")

        return ",".join(result)

    return countries.apply(convert)
