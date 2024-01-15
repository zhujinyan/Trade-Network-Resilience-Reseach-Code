import comtradeapicall
import os
import json
import pandas as pd
import time

'''
primary key: afaafd0c8d2647e3b0aa2a8303c931e8
second key: afaafd0c8d2647e3b0aa2a8303c931e8
'''


regions = ["USA", "China", "Japan", "Germany", "United Kingdom", "India", "France", "Italy", "Canada", "Rep. of Korea",
               "Russian Federation",
               "Brazil", "Australia", "Spain", "Mexico", "Indonesia", "Netherlands", "Saudi Arabia", "Turkey",
               "Switzerland", "Poland",
               "Sweden", "Belgium", "Thailand", "Ireland", "Argentina", "Norway", "Israel", "Austria", "Nigeria",
               "South Africa",
               "Bangladesh", "Egypt", "Denmark", "Singapore", "Philippines", "Malaysia", "China, Hong Kong SAR", "Viet Nam",
               "United Arab Emirates", "Pakistan",
               "Chile", "Colombia", "Finland", "Romania", "Czechia", "New Zealand", "Portugal", "Iran", "Peru"]

subscription_key = '279736592a3b4146ab883a01e98afcc3'
main_directory = 'data'


def crawler(hs, period):
    directory = os.path.join(main_directory, str(hs))
    if not os.path.exists(directory):
        os.mkdir(directory)

    # get countries' name and code
    with open("./reporterAreas.json", "r", encoding="utf_8_sig") as file:
        code_sheet = json.load(file)
        results = code_sheet.get("results")
        _id = []
        name = []
        for country in results:
            if country.get("text") in regions:
                _id.append(country.get("id"))
                name.append(country.get("text"))
            if len(_id) == 50:
                break

    # print(comtradeapicall.getFinalData(subscription_key, typeCode='C', freqCode='M', clCode='HS', period='202205,202206',
    #                                 reporterCode=32, cmdCode='2701', flowCode='M', partnerCode=842,
    #                                 partner2Code=246,
    #                                 customsCode=None, motCode=None, maxRecords=2500, format_output='JSON',
    #                                 aggregateBy=None, breakdownMode='classic', countOnly=None, includeDesc=True))

    for reporter in regions:
        for p, partner in enumerate(regions):
            if p % 2 == 1:
                continue

            df = comtradeapicall.getFinalData(subscription_key, typeCode='C', freqCode='M', clCode='HS', cmdCode=str(hs), flowCode='M',
                                        period=period, reporterCode=_id[name.index(reporter)], partnerCode=_id[name.index(partner)],
                                        partner2Code=_id[name.index(regions[p+1])],
                                        customsCode=None, motCode=None, maxRecords=2500, format_output='JSON',
                                        aggregateBy=None, breakdownMode='classic', countOnly=None,includeDesc=True)
            # print(df)
            time.sleep(10)
            if df is not None and list(df.columns.values) != []:
                filename = f'{reporter}_to_{partner}&{regions[p+1]}.csv'
                df.to_csv(os.path.join(directory, filename))


if __name__ == '__main__':
    period = ''

    year = '2022'
    for month in range(1, 13):
        if month < 10:
            period += year + '0' + str(month) + ','
        else:
            period += year + str(month) + ','

    for hs in [2701, 2704, 2709, 2711, 2716]:
        crawler(hs, period[:-1])
