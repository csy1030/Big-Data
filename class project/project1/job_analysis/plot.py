import plotly.graph_objects as go
import pandas as pd
from plotly.offline import init_notebook_mode, iplot
import plotly.express as px
import config

"""
same career: salary-location, demand-location
same location: demand-career, salary-career
salary-working experiences, salary-degree
"""


class RelationsPlot:
    def __init__(self):
        self.job_list = config.job_list_salary

    def same_job_diff_loc(self):
        for job in self.job_list:
            job = ''.join(job.split(" ")).replace('/', '')
            df = pd.read_csv('./salary_csv/job/{}_salary.csv'.format(job))
            mymap = go.Densitymapbox(lat=df.lat, lon=df.lon, z=df.salary_med)
            fig = go.Figure(mymap)
            token = 'pk.eyJ1IjoiY3N5MTAzMCIsImEiOiJja2NjaXlnZjYwNTJ1Mnluemx1dzlzZnd2In0.5u5tGKbh0fNfzkiij9z6hg'
            fig.update_layout(mapbox={'accesstoken': token, 'center': {'lon': -97.41107, 'lat': 42.03271}, 'zoom': 3.2},
                              margin={'l': 0, 'r': 0, 't': 0, 'b': 0},
                              # )
                              mapbox_style='dark')
            fig.write_html('./map_html/{}.html'.format(job))

    def same_loc_diff_job(self):
        # for loc in config.location_dict:
        #     item = "_".join(loc.split(",")[0].split(" "))
        item= 'Seattle'
        df = pd.read_csv('./salary_csv/location/{}.csv'.format(item))

        bar1 = px.bar(df, x='job', y='salary_med',
                      # text=df['demand'], textposition='outside',
                      labels={'salary_med': 'salary_median',
                              'job': item,
                              },
                      color='demand',
                      template='plotly_dark',

                      )

        fig = go.Figure(bar1)
        fig.write_html('./loc_bar/{}.html'.format(item))


if __name__ == '__main__':
    plot = RelationsPlot()
    plot.same_job_diff_loc()
