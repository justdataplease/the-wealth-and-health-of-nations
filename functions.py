import pandas as pd


def download_data():
    # Download data
    # https://www.gapminder.org/data/
    # https://github.com/open-numbers/ddf--gapminder--systema_globalis

    total_population = pd.read_csv(
        "https://raw.githubusercontent.com/open-numbers/ddf--gapminder--systema_globalis/master/countries-etc-datapoints/ddf--datapoints--population_total--by--geo--time.csv")
    life_expectancy_years = pd.read_csv(
        "https://raw.githubusercontent.com/open-numbers/ddf--gapminder--systema_globalis/master/countries-etc-datapoints/ddf--datapoints--life_expectancy_years--by--geo--time.csv")
    income_per_person = pd.read_csv(
        "https://raw.githubusercontent.com/open-numbers/ddf--gapminder--systema_globalis/master/countries-etc-datapoints/ddf--datapoints--income_per_person_gdppercapita_ppp_inflation_adjusted--by--geo--time.csv")
    geo_countries = pd.read_csv(
        "https://raw.githubusercontent.com/open-numbers/ddf--gapminder--systema_globalis/master/ddf--entities--geo--country.csv"
    )
    geo_countries['world_6region'] = geo_countries['world_6region'].astype(str).apply(lambda x: x.replace("_", " ").title())
    geo_countries['world_4region'] = geo_countries['world_4region'].astype(str).apply(lambda x: x.replace("_", " ").title())

    # Create initial dataframes to handle missing values
    countries = pd.DataFrame(life_expectancy_years.geo.unique(), columns=['geo'])
    years = pd.DataFrame(list(range(1800, 2023)), columns=['time'])
    init = countries.merge(years, how='cross')

    # Merge dataframe
    merged_data = init.merge(life_expectancy_years, how='left', on=["geo", "time"])
    merged_data = merged_data.merge(income_per_person, how='left', on=["geo", "time"])
    merged_data = merged_data.merge(total_population, how='left', on=["geo", "time"])
    merged_data = merged_data.merge(geo_countries[['country', 'income_groups', 'name', 'world_4region', 'world_6region']], how='left', left_on=["geo"],
                                    right_on=["country"])
    merged_data.drop(['country', 'geo'], axis=1, inplace=True)

    # Rename columns
    merged_data.rename(
        columns={'name': 'country', 'time': 'year', 'population_total': 'population',
                 'income_per_person_gdppercapita_ppp_inflation_adjusted': 'gdpPercap',
                 'life_expectancy_years': 'lifeExp', 'world_4region': 'continent', 'world_6region': 'region'}, inplace=True)
    merged_data = merged_data.sort_values(by=['year'], ascending=True)

    # Drop countries with null values
    counties_with_missing_data = merged_data[(merged_data['lifeExp'].isna()) | (merged_data['gdpPercap'].isna()) | (merged_data['population'].isna())][
        'country'].unique()
    merged_data = merged_data[~merged_data.country.isin(counties_with_missing_data)]
    return merged_data
