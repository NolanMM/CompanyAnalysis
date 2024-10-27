from plotly.subplots import make_subplots
import plotly.graph_objs as go

class Gold_Tables_Graphs:
    def plot_actual_gdp(self, dates, gdp_gold):
        """
        Plot actual GDP over time.
        """
        return go.Scatter(x=dates, y=gdp_gold, mode='lines', name='Actual GDP')

    def plot_forecasted_gdp(self, dates, forecasted_gdp):
        """
        Plot forecasted GDP over time.
        """
        return go.Scatter(x=dates, y=forecasted_gdp, mode='lines', name='Forecasted GDP')

    def plot_forecasted_inflation(self, dates, forecasted_inflation):
        """
        Plot forecasted inflation over time.
        """
        return go.Scatter(x=dates, y=forecasted_inflation, mode='lines', name='Forecasted Inflation')

    def plot_unemployment_rate(self, dates, unemployment_rate):
        """
        Plot unemployment rate over time.
        """
        return go.Scatter(x=dates, y=unemployment_rate, mode='lines', name='Unemployment Rate')

    def plot_time_series(self, data):
        """
        Generate a complete Plotly graph for time series analysis of GDP and inflation data.
        """
        dates = [row['DATETIME'] for row in data]
        gdp_gold = [row['GDP_GOLD'] for row in data]
        forecasted_gdp = [row['FORECASTED_GDP'] for row in data]
        forecasted_inflation = [row['FORECASTED_INFLATION'] for row in data]
        unemployment_rate = [row['UNEMPLOYMENT_RATE_GOLD'] for row in data]
        fig = make_subplots(specs=[[{"secondary_y": True}]])
        fig.add_trace(self.plot_actual_gdp(dates, gdp_gold), secondary_y=False)
        fig.add_trace(self.plot_forecasted_gdp(dates, forecasted_gdp), secondary_y=False)
        fig.add_trace(self.plot_forecasted_inflation(dates, forecasted_inflation), secondary_y=True)
        fig.add_trace(self.plot_unemployment_rate(dates, unemployment_rate), secondary_y=True)
        fig.update_layout(
            title='Time Series Analysis of Macroeconomic Data',
            xaxis=dict(title='Date'),
            yaxis=dict(title='GDP Values', side='left'),
            yaxis2=dict(title='Inflation/Unemployment Rate', side='right', overlaying='y')
        )
        return fig