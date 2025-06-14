import pandas as pd
import plotly.graph_objects as go
from dash import Dash, dcc, html, Input, Output, State  
import psycopg2
import logging
import base64
import os
from db_config import DB_CONFIG
from data_transfer import process_excel_to_postgres
from data_transfer_air import process_excel_to_postgres_air


# Настройка логирования
logging.basicConfig(level=logging.INFO)
logger = logging.getLogger(__name__)

def load_data_from_db():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        transport_query = """
        SELECT 
            Адрес,
            Время,
            Скорость,
            Поток,
            Широта AS "lat",
            Долгота AS "lon",
            Дата AS "date"
        FROM transport_metrics
        """
        df = pd.read_sql(transport_query, conn)
        return df
    except Exception as e:
        logger.error(f"Ошибка при загрузке данных из БД: {e}")
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()

def load_pollution_data():
    try:
        conn = psycopg2.connect(**DB_CONFIG)
        pollution_query = """
        SELECT 
            Адрес,
            Время,
            co,
            no,
            no2,
            so2,
            Дата AS "date"
        FROM air_pollution
        """
        df = pd.read_sql(pollution_query, conn)
        return df
    except Exception as e:
        logger.error(f"Ошибка при загрузке экологических данных: {e}")
        raise
    finally:
        if 'conn' in locals() and conn:
            conn.close()

# Загрузка и обработка данных
try:
    df = load_data_from_db()
    df["Время"] = pd.to_datetime(df["Время"], format="%H:%M:%S", errors="coerce").dt.strftime("%H:%M")
    df["date"] = pd.to_datetime(df["date"])
    df["Скорость"] = df["Скорость"].round(3)
    df["Поток"] = df["Поток"].round(0).astype(int)
    pollution_df = load_pollution_data()
    pollution_df["Время"] = pd.to_datetime(pollution_df["Время"], format="%H:%M:%S", errors="coerce").dt.strftime("%H:%M")
    pollution_df["date"] = pd.to_datetime(pollution_df["date"])

    min_date = df["date"].min().date()
    max_date = df["date"].max().date()
    
except Exception as e:
    logger.error(f"Ошибка при обработке данных: {e}")
    raise


app = Dash(__name__)


app.layout = html.Div([
    html.H1("Анализ транспортного потока и загрязнений", style={"textAlign": "center"}),

    html.Div([
        html.Label("Выберите период:"),
        dcc.DatePickerRange(
            id='date-picker',
            min_date_allowed=min_date,
            max_date_allowed=max_date,
            start_date=min_date,
            end_date=max_date,
            display_format='YYYY-MM-DD'
        )
    ], style={"marginBottom": "20px"}),

    html.Div([
        html.Label("Выберите адрес (транспорт):"),
        dcc.Dropdown(id="address-dropdown", options=[], value=None, clearable=False)
    ], style={"marginBottom": "30px"}),

    html.H3("График скорости и потока транспорта"),
    dcc.Graph(id="comparison-graph", style={"height": "450px"}),

    html.H3("Географическая карта"),
    dcc.Graph(id="map-graph", style={"height": "700px"}),

    html.H3("Наиболее загруженные участки"),
    dcc.Graph(id="top-flow-graph"),

    html.H3("Участки с минимальной скоростью"),
    dcc.Graph(id="low-speed-graph"),

    html.Hr(),
    html.H2("Анализ загрязнителей воздуха", style={"marginTop": "40px"}),

    html.Div([
        html.Label("Выберите адрес (загрязнение):"),
        dcc.Dropdown(
            id="pollution-address-dropdown",
            options=[{"label": addr, "value": addr} for addr in pollution_df["Адрес"].unique()],
            value=pollution_df["Адрес"].unique()[0],
            clearable=False
        )
    ], style={"marginBottom": "20px"}),

    html.Div([
        html.Label("Выберите типы загрязнителей:"),
        dcc.Checklist(
            id="pollutant-selector",
            options=[
                {"label": "CO", "value": "co"},
                {"label": "NO", "value": "no"},
                {"label": "NO₂", "value": "no2"},
                {"label": "SO₂", "value": "so2"},
            ],
            value=["co", "no", "no2", "so2"],
            labelStyle={"display": "inline-block", "marginRight": "15px"}
        )
    ], style={"marginBottom": "30px"}),

    dcc.Graph(id="pollution-graph"),

    html.Hr(),


    # Загрузка Excel-файла — перемещена в самый низ


html.Div([
    html.H2("Загрузка данных", style={"marginTop": "40px"}),

    html.H3("Загрузка транспортных данных"),
    dcc.Upload(
        id='upload-traffic-data',
        children=html.Div(['Перетащите или выберите Excel-файл с транспортными данными']),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'marginBottom': '20px'
        },
        multiple=False
    ),
    html.Div(id='traffic-upload-status'),

    html.H3("Загрузка данных о загрязнении воздуха"),
    dcc.Upload(
        id='upload-pollution-data',
        children=html.Div(['Перетащите или выберите Excel-файл с экологическими данными']),
        style={
            'width': '100%',
            'height': '60px',
            'lineHeight': '60px',
            'borderWidth': '1px',
            'borderStyle': 'dashed',
            'borderRadius': '5px',
            'textAlign': 'center',
            'marginBottom': '20px'
        },
        multiple=False
    ),
    html.Div(id='pollution-upload-status'),
    html.Hr()
    ])
], style={"width": "90%", "margin": "0 auto", "padding": "20px"})






@app.callback(
    Output("address-dropdown", "options"),
    Output("address-dropdown", "value"),
    
    Input("date-picker", "start_date"),
    Input("date-picker", "end_date")
)
def update_address_dropdown(start_date, end_date):
    filtered_df = df[(df["date"] >= pd.to_datetime(start_date)) & 
                     (df["date"] <= pd.to_datetime(end_date))]
    addresses = filtered_df["Адрес"].unique()
    options = [{"label": addr, "value": addr} for addr in addresses]
    value = addresses[0] if len(addresses) > 0 else None
    return options, value

@app.callback(
    Output("comparison-graph", "figure"),
    Output("map-graph", "figure"),
    Output("top-flow-graph", "figure"),
    Output("low-speed-graph", "figure"),
    Input("address-dropdown", "value"),
    Input("date-picker", "start_date"),
    Input("date-picker", "end_date")
)
def update_graphs(selected_address, start_date, end_date):
    if not selected_address:
        return go.Figure(), go.Figure()
    
    filtered_df = df[(df["date"] >= pd.to_datetime(start_date)) & 
                     (df["date"] <= pd.to_datetime(end_date))]

    grouped = filtered_df.groupby(["Адрес", "Время", "lat", "lon"]).agg({
        "Скорость": "mean",
        "Поток": "sum"
    }).reset_index()


    df_top10 = df.groupby("Адрес").agg({"Поток": "sum"}).reset_index()
    df_top10 = df_top10.sort_values("Поток", ascending=False).head(10)
    top_addresses = df_top10["Адрес"].tolist()

    df_speed = df[df["Скорость"] > 0]
    low_speed_addresses = df_speed.groupby("Адрес")["Скорость"].mean().nsmallest(10).index
    df_low_speed = df[df["Адрес"].isin(low_speed_addresses)]

    high_speed_threshold = df["Скорость"].quantile(0.95)
    high_flow_threshold = df["Поток"].quantile(0.95)
    risky_points = df[(df["Скорость"] >= high_speed_threshold) & 
                                (df["Поток"] >= high_flow_threshold)]
    risky_sample = risky_points.sample(n=min(10, len(risky_points)), random_state=42)


    dff = grouped[grouped["Адрес"] == selected_address]
    fig_graph = go.Figure()
    fig_graph.add_trace(go.Bar(x=dff["Время"], y=dff["Поток"], name="Поток", marker_color="orange"))
    fig_graph.add_trace(go.Scatter(x=dff["Время"], y=dff["Скорость"], name="Скорость", yaxis="y2", line=dict(color="#4682B4", width=3)))

    fig_graph.update_layout(
        title=f"Скорость и поток на адресе: {selected_address}",
        xaxis_title="Время",
        yaxis=dict(title="Поток (авто/ч)",showgrid=True, gridcolor="#080808"),
        yaxis2=dict(title="Скорость (км/ч)", overlaying="y", side="right"),
        plot_bgcolor="#f9f9f9",
        paper_bgcolor="#f4f4f4"
    )

     
    top_flow_df = filtered_df.groupby("Адрес")["Поток"].sum().nlargest(10).reset_index()
    fig_top_flow = go.Figure()
    fig_top_flow.add_trace(go.Bar(
        x=top_flow_df["Поток"],
        y=top_flow_df["Адрес"],
        orientation="h",
        marker_color="#FF7F0E",
        text=top_flow_df["Поток"],
        textposition="auto"
    ))
    fig_top_flow.update_layout(
        xaxis_title="Суммарный поток (авто)",
        yaxis_title="Адрес",
        yaxis=dict(autorange="reversed"),
        plot_bgcolor="#f9f9f9",
        paper_bgcolor="#f4f4f4"
    )
    
    # 2. График ТОП-10 участков с наименьшей скоростью
    low_speed_df = filtered_df[filtered_df["Скорость"] > 0].groupby("Адрес")["Скорость"].mean().nsmallest(10).reset_index()
    fig_low_speed = go.Figure()
    fig_low_speed.add_trace(go.Bar(
        x=low_speed_df["Скорость"],
        y=low_speed_df["Адрес"],
        orientation="h",
        marker_color="#1f77b4",
        text=[f"{v:.2f}" for v in low_speed_df["Скорость"]],
        textposition="auto"
    ))
    fig_low_speed.update_layout(
        xaxis_title="Средняя скорость (км/ч)",
        yaxis_title="Адрес",
        yaxis=dict(autorange="reversed"),
        plot_bgcolor="#f9f9f9",
        paper_bgcolor="#f4f4f4"
    )


    # Карта
    fig_map = go.Figure()
    
    # Топ-10 адресов
    df_top_points = filtered_df[filtered_df["Адрес"].isin(top_addresses)]
    fig_map.add_trace(go.Scattermapbox(
        lat=df_top_points["lat"],
        lon=df_top_points["lon"],
        mode="markers",
        marker=dict(size=18, color="darkred", opacity=0.9),
        text=df_top_points.apply(lambda x: f"{x['Адрес']}<br>Поток: {x['Поток']}", axis=1),
        textposition="top right",
        name="Наиболее загруженные участки"
    ))
    
    # Адреса с низкой скоростью
    fig_map.add_trace(go.Scattermapbox(
        lat=df_low_speed["lat"],
        lon=df_low_speed["lon"],
        mode="markers",
        marker=dict(size=18, color="navy", opacity=0.8),
        text=df_low_speed.apply(lambda x: f"{x['Адрес']}<br>Скорость: {x['Скорость']} км/ч", axis=1),
        textposition="top right",
        name="Адресы с низкой средней скоростью"
    ))
    
    # Рискованные точки
    fig_map.add_trace(go.Scattermapbox(
        lat=risky_sample["lat"],
        lon=risky_sample["lon"],
        mode="markers",
        marker=dict(size=18, color="black", opacity=0.8),
        text=risky_sample.apply(lambda x: f"Аварийный риск<br>{x['Адрес']}<br>Скорость: {x['Скорость']:.1f} км/ч<br>Поток: {x['Поток']}", axis=1),
        hoverinfo="text",
        name="Потенциально аварийные участки"
    ))
    
    # Все адреса
    fig_map.add_trace(go.Scattermapbox(
        lat=filtered_df['lat'],
        lon=filtered_df['lon'],
        mode='markers',
        marker=dict(
            size=15,
            color=filtered_df['Скорость'],
            cmin=30,
            cmax=80,
            showscale=True,
            opacity=0.8
        ),
        text=filtered_df.apply(lambda x: f"{x['Адрес']}<br>Скорость: {x['Скорость']} км/ч<br>Поток: {x['Поток']} авто", axis=1),
        hoverinfo='text',
        name='Адреса'
    ))
    
    # Подсветка выбранного адреса
    selected = filtered_df[filtered_df['Адрес'] == selected_address]
    if not selected.empty:
        fig_map.add_trace(go.Scattermapbox(
            lat=selected['lat'],
            lon=selected['lon'],
            mode='markers',
            marker=dict(size=15, color='red'),
            name='Выбранный адрес'
        ))
    
    fig_map.update_layout(
        mapbox_style="open-street-map",
        mapbox=dict(
            center=dict(lat=filtered_df['lat'].mean(), lon=filtered_df['lon'].mean()),
            zoom=11
        ),
        margin={"r":0,"t":0,"l":0,"b":0},
        legend=dict(x=0, y=1),
        plot_bgcolor="#f9f9f9",
        paper_bgcolor="#f4f4f4"
    )
    

    return fig_graph, fig_map, fig_top_flow, fig_low_speed

@app.callback(
    Output("pollution-graph", "figure"),
    Input("pollution-address-dropdown", "value"),
    Input("pollutant-selector", "value"),
    Input("date-picker", "start_date"),
    Input("date-picker", "end_date")
)
def update_pollution_graph(pollution_address, selected_pollutants, start_date, end_date):
    filtered_pollution = pollution_df[
        (pollution_df["Адрес"] == pollution_address) &
        (pollution_df["date"] >= pd.to_datetime(start_date)) &
        (pollution_df["date"] <= pd.to_datetime(end_date))
    ]
    fig = go.Figure()
    if not selected_pollutants:
        fig.update_layout(title="Выберите хотя бы один загрязнитель")
        return fig

    colors = {"co": "#8B0000", "no": "#FF8C00", "no2": "#4682B4", "so2": "#2E8B57"}
    for pol in selected_pollutants:
        fig.add_trace(go.Scatter(
            x=filtered_pollution["Время"],
            y=filtered_pollution[pol],
            mode="lines+markers",
            name=pol.upper(),
            line=dict(width=2),
            marker_color=colors[pol]
        ))

    fig.update_layout(
        title=f"Динамика загрязнителей на адресе: {pollution_address}",
        xaxis_title="Время",
        yaxis_title="Концентрация",
        plot_bgcolor="#f9f9f9",
        paper_bgcolor="#f4f4f4"
    )
    return fig


UPLOAD_FOLDER = "uploaded_files"
os.makedirs(UPLOAD_FOLDER, exist_ok=True)
@app.callback(
    Output('traffic-upload-status', 'children'),
    Input('upload-traffic-data', 'contents'),
    State('upload-traffic-data', 'filename')
)
def handle_file_upload(contents, filename):
    if contents is None:
        return ""

    try:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)

        # Сохраняем файл во временную папку
        file_path = os.path.join(UPLOAD_FOLDER, filename)
        with open(file_path, "wb") as f:
            f.write(decoded)

        # Запускаем функцию обработки файла
        result = process_excel_to_postgres(file_path)

        if result:
            return f"✅ Файл {filename} успешно загружен и обработан."
        else:
            return f"❌ Ошибка при обработке файла {filename}. Проверьте содержимое."

    except Exception as e:
        return f"⚠️ Ошибка: {str(e)}"
    

@app.callback(
    Output('pollution-upload-status', 'children'),
    Input('upload-pollution-data', 'contents'),
    State('upload-pollution-data', 'filename')
)
def handle_pollution_upload(contents, filename):
    if contents is None:
        return ""

    try:
        content_type, content_string = contents.split(',')
        decoded = base64.b64decode(content_string)

        file_path = os.path.join(UPLOAD_FOLDER, filename)
        with open(file_path, "wb") as f:
            f.write(decoded)

        result = process_excel_to_postgres_air(file_path)

        if result:
            return f"✅ Файл {filename} успешно загружен и обработан (экология)."
        else:
            return f"❌ Ошибка при обработке экологического файла {filename}. Проверьте содержимое."
    except Exception as e:
        return f"⚠️ Ошибка загрузки экологического файла: {str(e)}"



if __name__ == "__main__":
    app.run(debug=True)
