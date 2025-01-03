import streamlit as st
import sqlite3
import os
import pandas as pd
import plotly.express as px
from scipy.stats import spearmanr

# Список бд из текущей директории
def get_db_files():
    return [f for f in os.listdir() if f.endswith('.db')]

# Подключение
def connect_to_db(db_name):
    try:
        conn = sqlite3.connect(db_name, check_same_thread=False)
        st.session_state['conn'] = conn
        st.toast(f"Подключено к {db_name}", icon="✅")
    except Exception as e:
        st.toast(f"Ошибка подключения: {e}", icon="🚨")


def disconnect_db():
    if 'conn' in st.session_state and st.session_state['conn']:
        st.session_state['conn'].close()
        st.session_state['conn'] = None
        st.toast("Соединение закрыто", icon="🚨")


def execute_query(query):
    try:
        if 'conn' not in st.session_state or not st.session_state['conn']:
            st.toast("Нет подключения к базе данных!", icon="🚨")
            return None

        conn = st.session_state['conn']
        cursor = conn.cursor()
        cursor.execute(query)
        conn.commit()
        data = cursor.fetchall()
        columns = [desc[0]
                   for desc in cursor.description] if cursor.description else []
        st.toast(f"Запрос выполнен успешно", icon="✅")
        return pd.DataFrame(data, columns=columns) if data else None
    except Exception as e:
        st.error(f"Ошибка выполнения запроса: {e}")
        return None


# Функция для визуализации
def visualize_data(topic):
    if 'conn' not in st.session_state or not st.session_state['conn']:
        st.toast("Сначала подключитесь к базе данных!", icon="🚨")
        return

    conn = st.session_state['conn']

    @st.cache_data
    def fetch_data(query):
        try:
            return pd.read_sql_query(query, conn)
        except Exception as e:
            st.error(f"Ошибка выполнения запроса: {e}")
    # Изменение фона ячейка в рамках корреляции 
    def highlight_max(val, column_name):
        if column_name == 'Комментарии-Продолжительность':
            max_value = correlation_df['Комментарии-Продолжительность'].max()
        elif column_name == 'Лайки-Продолжительность':
            max_value = correlation_df['Лайки-Продолжительность'].max()
        color = '#292929'
        if val == max_value:
            color = '#006400'
        return f'background-color: {color}'

    if topic == "Каналы":
        videos_df = fetch_data('''
        SELECT Videos.ID, Videos.Title, Channels.Title AS Channel, 
               Videos.LikeCount, Videos.DislikeCount, Videos.ViewCount, Videos.CommentCount, 
               Videos.CreationDate, Videos.Duration, Projects.Title AS Project
        FROM Videos
        JOIN Channels ON Videos.Channel_ID = Channels.ID
        LEFT JOIN Projects ON Videos.Project_ID = Projects.ID; 
        ''')

        comments_df = fetch_data('''
        SELECT Comments.Video_ID, Comments.CreationDate, Comments.Sentiment
        FROM Comments
        JOIN Videos ON Comments.Video_ID = Videos.ID;
        ''')

        st.header("Статистика по видео и комментариям")

        videos_df['CreationDate'] = pd.to_datetime(videos_df['CreationDate'])
        comments_df['CreationDate'] = pd.to_datetime(
            comments_df['CreationDate'])
        # Выбор каналов через unique
        selected_channel = st.selectbox(
            "Выберите канал:", videos_df['Channel'].unique())
        filtered_videos = videos_df[videos_df['Channel'] == selected_channel]

        avg_views = filtered_videos['ViewCount'].mean()
        avg_likes = filtered_videos['LikeCount'].mean()
        avg_dislikes = filtered_videos['DislikeCount'].mean()
        avg_comments = filtered_videos['CommentCount'].mean()

        total1, total2, total3, total4 = st.columns(4, gap='small')

        with total1:
            st.info('Среднее количество просмотров', icon="👀")
            st.metric(label="Просмотры:", value=f"{avg_views:,.0f}")
        with total2:
            st.info('Среднее количество лайков', icon="👍")
            st.metric(label="Лайки:", value=f"{avg_likes:,.0f}")

        with total3:
            st.info('Среднее количество дизлайков', icon="👎")
            st.metric(label="Дизлайки:", value=f"{avg_dislikes:,.0f}")

        with total4:
            st.info('Среднее количество комментариев', icon="💬")
            st.metric(label="Комментарии:", value=f"{avg_comments:,.0f}")

        video_ids = filtered_videos['ID'].unique()
        filtered_comments = comments_df[comments_df['Video_ID'].isin(
            video_ids)]
        # Суммирование по месяцам
        videos_grouped = filtered_videos.resample(
            'M', on='CreationDate').sum().reset_index()

        comments_grouped = filtered_comments.groupby([
            pd.Grouper(key='CreationDate', freq='M'), 'Sentiment']
        ).size().reset_index(name='Count')

        left, right = st.columns(2)

        with left:
            fig_metrics_by_month = px.line(
                videos_grouped,
                x='CreationDate',
                y=['ViewCount', 'LikeCount', 'DislikeCount', 'CommentCount'],
                labels={
                    'CreationDate': 'Дата',
                    'value': 'Количество',
                    'variable': 'Показатель'
                },
                title="Динамика охватов видео канала",
                markers=True)

            fig_metrics_by_month.update_layout(
                hovermode="x unified", xaxis_title=None)

            st.plotly_chart(fig_metrics_by_month)

        with right:
            sentiment_mapping = {0: "Нейтральный",
                                 1: "Позитивный",
                                 2: "Негативный"}
            comments_grouped['Sentiment'] = comments_grouped['Sentiment'].map(
                sentiment_mapping)

            fig_sentiment_by_month = px.line(
                comments_grouped,
                x='CreationDate',
                y='Count',
                color='Sentiment',
                labels={
                    'CreationDate': 'Дата',
                    'Count': 'Количество комментариев',
                    'Sentiment': 'Тональность'},
                title="Динамика комментариев по тональности",
                markers=True)

            fig_sentiment_by_month.update_layout(
                legend_title_text="Сентимент",
                xaxis_title=None,
                yaxis_title=None,
                hovermode="x unified")

            st.plotly_chart(fig_sentiment_by_month)

        scatter_left, scatter_right = st.columns(2)

        with scatter_left:
            fig_duration_comments = px.scatter(
                filtered_videos,
                x='Duration', y='CommentCount',
                color='Project',
                labels={'Duration': 'Продолжительность (сек.)',
                        'CommentCount': 'Количество комментариев',
                        'Project': 'Проект'},
                title="Связь комментариев и продолжительности видео",
                size_max=30)
            st.plotly_chart(fig_duration_comments)

        with scatter_right:
            fig_duration_likes = px.scatter(
                filtered_videos,
                x='Duration', y='LikeCount',
                color='Project',
                labels={'Duration': 'Продолжительность (сек.)',
                        'LikeCount': 'Количество лайков',
                        'Project': 'Проект'},
                title="Связь лайков и продолжительности видео",
                size_max=30)
            st.plotly_chart(fig_duration_likes)

        correlation_results = []

        for project in filtered_videos['Project'].dropna().unique():
            project_videos = filtered_videos[filtered_videos['Project'] == project]
            if len(project_videos) > 1:
                corr_comments, p_comments = spearmanr(
                    project_videos['Duration'], project_videos['CommentCount'])
                corr_likes, p_likes = spearmanr(
                    project_videos['Duration'], project_videos['LikeCount'])
                correlation_results.append({
                    'Проекты': project,
                    'Комментарии-Продолжительность': f"{corr_comments:.2f}{'*' if p_comments < 0.05 else ''}",
                    'Лайки-Продолжительность': f"{corr_likes:.2f}{'*' if p_likes < 0.05 else ''}"})

        correlation_df = pd.DataFrame(correlation_results)

        st.write("### Корреляции по проектам")

        styled_correlation_df = correlation_df.style.map(
            lambda val: highlight_max(val, 'Комментарии-Продолжительность'),
            subset=['Комментарии-Продолжительность']).map(lambda val: highlight_max(val, 'Лайки-Продолжительность'),
                                                          subset=['Лайки-Продолжительность'])

        st.dataframe(styled_correlation_df, use_container_width=True)

    elif topic == "Видео":
        videos_df = fetch_data('''
        SELECT Videos.ID, Videos.Title, Channels.Title AS Channel, 
               Videos.LikeCount, Videos.DislikeCount, Videos.ViewCount, Videos.CommentCount,
               strftime('%Y', Videos.CreationDate) AS Year,
               Projects.Title AS Project_Title
        FROM Videos
        JOIN Channels ON Videos.Channel_ID = Channels.ID
        LEFT JOIN Projects ON Videos.Project_ID = Projects.ID; ''')

        st.header("Топ-10 видео по ключевым метрикам")

        selected_channel = st.selectbox(
            "Выберите канал:", videos_df['Channel'].unique())

        filtered_videos = videos_df[videos_df['Channel'] == selected_channel]

        min_year, max_year = filtered_videos['Year'].astype(
            int).min(), filtered_videos['Year'].astype(int).max()

        selected_year = st.slider(
            "Выберите диапазон:", min_value=min_year,
            max_value=max_year, value=(min_year, max_year), step=1)

        filtered_videos = filtered_videos[
            (filtered_videos['Year'].astype(int) >= selected_year[0]) &
            (filtered_videos['Year'].astype(int) <= selected_year[1])]

        top_views = filtered_videos.nlargest(10, 'ViewCount')
        top_comments = filtered_videos.nlargest(10, 'CommentCount')
        top_likes = filtered_videos.nlargest(10, 'LikeCount')
        top_dislikes = filtered_videos.nlargest(10, 'DislikeCount')

        videos_df['Project_Title'] = videos_df['Project_Title'].fillna(
            'Без проекта')

        unique_projects = videos_df['Project_Title'].unique()
        colors = px.colors.qualitative.G10
        color_map = {project: colors[i % len(colors)]
                     for i, project in enumerate(unique_projects)}

        left_col, right_col = st.columns(2)

        with left_col:
            fig_views = px.bar(
                top_views,
                x='Title',
                y='ViewCount',
                color='Project_Title',
                labels={'Title': 'Видео', 'ViewCount': 'Просмотры',
                        'Project_Title': 'Проект'},
                title='Топ видео по просмотрам',
                category_orders={'Title': top_views['Title'].tolist()},
                color_discrete_map=color_map)

            fig_views.update_layout(
                showlegend=True,
                xaxis_title='Видео',
                xaxis=dict(showticklabels=False))

            st.plotly_chart(fig_views, use_container_width=True)

        with right_col:
            fig_comments = px.bar(
                top_comments,
                x='Title',
                y='CommentCount',
                color='Project_Title',
                labels={'Title': 'Видео', 'CommentCount': 'Комментарии',
                        'Project_Title': 'Проект'},
                title='Топ видео по комментариям',
                category_orders={'Title': top_comments['Title'].tolist()},
                color_discrete_map=color_map)

            fig_comments.update_layout(
                showlegend=True,
                xaxis_title='Видео',
                xaxis=dict(showticklabels=False)
            )
            st.plotly_chart(fig_comments, use_container_width=True)

        bottom_left, bottom_right = st.columns(2)

        with bottom_left:
            fig_likes = px.bar(
                top_likes,
                x='Title',
                y='LikeCount',
                color='Project_Title',
                labels={'Title': 'Видео', 'LikeCount': 'Лайки',
                        'Project_Title': 'Проект'},
                title='Топ видео по лайкам',
                category_orders={'Title': top_likes['Title'].tolist()},
                color_discrete_map=color_map)

            fig_likes.update_layout(
                showlegend=True,
                xaxis_title='Видео',
                xaxis=dict(showticklabels=False)
            )
            st.plotly_chart(fig_likes, use_container_width=True)

        with bottom_right:
            fig_dislikes = px.bar(
                top_dislikes,
                x='Title',
                y='DislikeCount',
                color='Project_Title',
                labels={'Title': 'Видео', 'DislikeCount': 'Дизлайки',
                        'Project_Title': 'Проект'},
                title='Топ видео по дизлайкам',
                category_orders={'Title': top_dislikes['Title'].tolist()},
                color_discrete_map=color_map)

            fig_dislikes.update_layout(
                showlegend=True,
                xaxis_title='Видео',
                xaxis=dict(showticklabels=False)
            )
            st.plotly_chart(fig_dislikes, use_container_width=True)

    elif topic == "Комментарии":
        comments_df = fetch_data('''
        SELECT Comments.ID, Comments.Text, Comments.LikeCount, Comments.Sentiment,
               Videos.Title AS VideoTitle, Channels.Title AS Channel,
               strftime('%Y', Comments.CreationDate) AS Year
        FROM Comments
        JOIN Videos ON Comments.Video_ID = Videos.ID
        JOIN Channels ON Videos.Channel_ID = Channels.ID; ''')

        st.header("Комментарии с наибольшим числом лайков")

        selected_channel = st.selectbox(
            "Выберите канал:", comments_df['Channel'].unique())

        filtered_comments = comments_df[comments_df['Channel']
                                        == selected_channel]

        min_year, max_year = filtered_comments['Year'].astype(
            int).min(), filtered_comments['Year'].astype(int).max()

        selected_years = st.slider(
            "Выберите диапазон годов:", min_year, max_year, (min_year, max_year), step=1)

        filtered_comments = filtered_comments[
            (filtered_comments['Year'].astype(int) >= selected_years[0]) &
            (filtered_comments['Year'].astype(int) <= selected_years[1])]

        positive_comments = filtered_comments[filtered_comments['Sentiment'] == 1]
        negative_comments = filtered_comments[filtered_comments['Sentiment'] == 2]

        top_positive = positive_comments.nlargest(10, 'LikeCount')
        top_negative = negative_comments.nlargest(10, 'LikeCount')

        st.subheader("Топ-10 позитивных комментариев")
        st.dataframe(top_positive[['Text', 'LikeCount', 'VideoTitle']],
                     use_container_width=True)

        st.subheader("Топ-10 негативных комментариев")
        st.dataframe(top_negative[['Text', 'LikeCount', 'VideoTitle']],
                     use_container_width=True)

    elif topic == "Проекты":
        incomes_df = fetch_data('''SELECT Projects.Title AS Project, Incomes.Amount, 
                                strftime('%Y', Incomes.CreationDate) AS Year, Incomes.Category
                                FROM Incomes JOIN Projects ON Incomes.Project_ID = Projects.ID; ''')

        costs_df = fetch_data(''' SELECT Projects.Title AS Project, Costs.Amount, 
                              strftime('%Y', Costs.CreationDate) AS Year, Costs.Category
                              FROM Costs JOIN Projects ON Costs.Project_ID = Projects.ID;''')

        st.header("Динамика проектов по доходам и расходам")

        min_year = min(incomes_df['Year'].min(), costs_df['Year'].min())
        max_year = max(incomes_df['Year'].max(), costs_df['Year'].max())

        selected_years = st.slider("Выберите диапазон:", int(min_year), int(
            max_year), (int(min_year), int(max_year)), step=1)

        filtered_incomes = incomes_df[incomes_df['Year'].astype(
            int).between(selected_years[0], selected_years[1])]
        filtered_costs = costs_df[costs_df['Year'].astype(
            int).between(selected_years[0], selected_years[1])]

        top_incomes = filtered_incomes.groupby(
            'Project')['Amount'].sum().reset_index().nlargest(10, 'Amount')
        top_costs = filtered_costs.groupby(
            'Project')['Amount'].sum().reset_index().nlargest(10, 'Amount')

        left_col, right_col = st.columns(2)

        with left_col:
            fig_incomes = px.bar(top_incomes, x='Amount', y='Project',
                                 orientation='h', color='Project',
                                 title="Топ проектов по доходам",
                                 labels={'Amount': 'Сумма доходов', 'Project': 'Проект'})

            fig_incomes.update_layout(
                xaxis_title=None, yaxis_title=None, showlegend=False)
            st.plotly_chart(fig_incomes, use_container_width=True)

            with right_col:
                fig_costs = px.bar(top_costs, x='Amount', y='Project',
                                   orientation='h', color='Project',
                                   title="Топ проектов по расходам",
                                   labels={'Amount': 'Сумма расходов', 'Project': 'Проект'})
                fig_costs.update_layout(xaxis_title=None,
                                        yaxis_title=None, showlegend=False)
                st.plotly_chart(fig_costs, use_container_width=True)

        income_category_sums = filtered_incomes.groupby(
            'Category')['Amount'].sum().reset_index()
        cost_category_sums = filtered_costs.groupby(
            'Category')['Amount'].sum().reset_index()

        left_col_pie, right_col_pie = st.columns(2)
        with left_col_pie:
            fig_income_pie = px.pie(income_category_sums,
                                    values='Amount', names='Category',
                                    title="Доходы по категориям", labels={'Amount': 'Сумма', 'Category': 'Категория'})
            st.plotly_chart(fig_income_pie, use_container_width=True)

        with right_col_pie:
            fig_cost_pie = px.pie(cost_category_sums, values='Amount', names='Category',
                                  title="Расходы по категориям", labels={'Amount': 'Сумма', 'Category': 'Категория'})
            st.plotly_chart(fig_cost_pie, use_container_width=True)


# Main
st.set_page_config(page_title="Интерактивный дашборд", layout="wide")

st.sidebar.image("data/medium_quality.png", use_container_width=True)

intro = """
### :sparkles: Проект по анализу YouTube-каналов продакшен-компании :orange[Medium Quality Production]

#### :heavy_check_mark: Цель проекта: аналитика популярности YouTube-каналов и доходности проектов продакшен-компании.

#### :heavy_check_mark: Задачи: 
##### :one: Визуализация основных статистик популярности каналов и доходности проектов.
##### :two: Изучение настроения аудитории каналов.
##### :three: Выявление лидирующих по показателям популярности видео на каналах.
##### :four: Выявление лидирующих по показателям доходности проектов.
##### :five: Анализ связи между характеристиками видео на каналах и показателями их популярности.

#### :book: Краткое описание данных и предметной области:
Данные о каналах, видео и их аудиторной активности были собраны с помощью YouTube API (скрипт для сбора данных был реализован на Python). 

Недоступные для парсинга данные (о финансовых показателях проектов, статьях расходов) были сгенерированы с помощью Python-библиотек Faker и random.

База данных предназначена для хранения данных об аудиторной активности, доходах и расходах YouTube-каналов, входящих в них проектов и содержащихся на них видео.

Фокус сделан на трех каналах: :blue[**LABELSMART**], :green[**easycom**] и :red[**LABELCOM**], позиционирующих себя как образовательно-развлекательные YouTube-каналы. 

В проекты (серии видео по определенной тематике) данных каналов входят: """

data_intro = {"Название проекта": ["Мир Смеха", "Горячие минуты", "Темные дела",
                                   "Аргументы и факты", "Ночная история",
                                   "Женский голос", "В эфире", "Легенды", "Сцена и юмор"],
              "Описание проекта": ["Проект о юморе и комедии.",
                                   "Проект о банях и отдыхе.",
                                   "Проект о криминале и расследованиях.",
                                   "Проект о фактах и аргументах.",
                                   "Проект об историях на ночь.",
                                   "Проект о женском стендапе и разговорах.",
                                   "Проект о подкастах и обсуждениях.",
                                   "Проект о биографиях великих людей.",
                                   "Проект о шоу и сценических выступлениях."
                                   ]
              }

df_intro = pd.DataFrame(data_intro)

intro_2 = '''#### Концептуальная и логическая модель нашей базы данных'''

conclusion = """
### :writing_hand: Основные выводы

##### :one: Вовлеченность:
Наибольшую вовлеченность имеет канал :green[easycom]. 
Канал предлагает различные развлекательные шоу с участием комиков, включая такие проекты, как **Натальная карта**, где обсуждаются натальные карты приглашенных гостей, и **«Женский форум»**, где рассматриваются вопросы с женских форумов в юмористическом ключе.

По результатам корреляционного анализа было выявлено, что в рамках проектов **Мир Cмеха** и **Темные дела** есть положительная значимая сильная связь между числом комментариев/лайков и продолжительностью видео.

##### :two: Динамика популярности проектов и их видео:
Наблюдается рост интереса к жанру **True Crime**. Это можно заметить в рамках канала :blue[LABELSMART] и видео с участием **Александры Сулим**.

У канала :red[LABELCOM] популярны видео, где активно участвуют знаменитости (**Яна Кошкина**, **Гарик Харламов**, **Артемий Лебедев**).

##### :three: Тональность комментариев:
По негативным комментариям у канала :red[LABELCOM] наблюдается критика относительно перехода из **YouTube** в **VK Video**.

##### :four: Прибыльность проектов:
Наиболее прибыльными являются следующие проекты: **Сцена и юмор**, **В эфире**, **Аргументы и факты** """

if 'conn' not in st.session_state:
    st.session_state['conn'] = None

st.sidebar.header("Структура")

section = st.sidebar.selectbox(
    "Раздел:", ["Введение", "Анализ", "Результаты"],
    placeholder="Выберите раздел",
    index=None)

if section == "Введение":
    st.markdown(intro)
    with st.expander("Описание проектов каналов"):
        st.dataframe(df_intro, use_container_width=True)
    st.markdown(intro_2)
    with st.expander("Изображения моделей"):
        pic1, pic2 = st.columns(2)
        with pic1:
            st.image("data/Conceptual_model.png",
                     caption="Концептуальная модель БД")
        with pic2:
            st.image("data/Logical_model.png",
                     caption="Логическая модель БД")
    st.markdown(
        '#### [Отчет по проекту](https://github.com/kovdanigor/youtube_db_project/blob/main/Отчет%20по%20проекту.pdf)')

elif section == "Анализ":
    st.sidebar.header("Настройки анализа")

    db_files = get_db_files()

    selected_db = st.sidebar.selectbox("База данных:",
                                       db_files, index=None,
                                       placeholder="Выберите базу данных")

    connection_action = st.sidebar.radio(
        "Действие:", options=["Подключение", "Отключение"],
        horizontal=True, index=None)

    if connection_action == "Подключение" and selected_db:
        if not st.session_state['conn']:
            connect_to_db(selected_db)
    elif connection_action == "Отключение":
        disconnect_db()

    if 'conn' in st.session_state and st.session_state['conn']:
        st.sidebar.subheader("Запросы")

        query_mode = st.sidebar.selectbox("Режим:", ["DDL/DML"],
                                          index=None, placeholder="Выберите режим")

        if query_mode == "DDL/DML":
            st.title("Запросы к БД")
            dml_query = st.text_area("Введите запрос:")
            if st.button("Выполнить запрос"):
                result_df = execute_query(dml_query)
                if result_df is not None:
                    st.dataframe(result_df, use_container_width=True)

            col1, col2 = st.columns(2)

            with col1:
                with st.expander("Проверка ограничений целостности"):
                    st.code('''
-- 1. Вставка и дублирование первичного ключа ID
PRAGMA foreign_keys = ON;
INSERT INTO Channels (ID, Title, Description, CreationDate, SubscriberCount) 
VALUES ('ch1', 'One', 'One', '2024-01-01', 10);

INSERT INTO Channels (ID, Title, Description, CreationDate, SubscriberCount) 
VALUES ('ch1', 'One', 'One', '2023-01-02', 2000);

-- 2. Вставка видео с несуществ. Channel_ID
INSERT INTO Videos (ID, Channel_ID, Project_ID, Title, Description, CreationDate, Duration, LikeCount, DislikeCount, CommentCount, ViewCount) 
VALUES ('vid1', 'none_channel', 1, 'Title', 'Description', '2024-01-01', 30, 10, 0, 5, 10);

-- 3. Вставка отрицательного значения SubscriberCount
INSERT INTO Channels (ID, Title, Description, CreationDate, SubscriberCount) 
VALUES ('ch2', 'One', 'Desc', '2024-01-03', -500);

-- 4. Вставка видео для канала ch1, затем удаление канала
INSERT INTO Videos (ID, Channel_ID, Project_ID, Title, Description, CreationDate, Duration, LikeCount, DislikeCount, CommentCount, ViewCount) 
VALUES ('vid2', 'ch1', NULL, 'Title', 'Description', '2024-12-27', 600, 0, 0, 2, 20);

DELETE FROM Channels WHERE ID = 'ch1';

-- Ничего не вернет
SELECT * FROM Videos WHERE Channel_ID = 'ch1';
                            ''', language="sql")

            with col2:
                with st.expander("Описание триггеров"):
                    st.code('''
-- 0. Получение списка триггеров
SELECT name, sql FROM sqlite_master WHERE type='trigger';
                            
SELECT ID, Title, Status FROM Projects WHERE ID=3;
                            
INSERT INTO Incomes (ID, Project_ID, Category, Counterparty, CreationDate, Amount) 
VALUES (31, 3, 'Продажа цифрового контента', 'Андреев Лимитед', '2019-11-09', 1477410+50000);
                            
-- 1. Триггер для доходов на вставку                            
CREATE TRIGGER AfterInsertIncomes
AFTER INSERT ON Incomes
FOR EACH ROW
BEGIN
    UPDATE Projects
    SET Status = CASE
        WHEN (SELECT SUM(Amount) FROM Incomes WHERE Project_ID = NEW.Project_ID) >
             (SELECT SUM(Amount) FROM Costs WHERE Project_ID = NEW.Project_ID)
        THEN 'Прибыльный'
        ELSE 'Убыточный'
    END
    WHERE ID = NEW.Project_ID;
END;
                            
-- 2. Триггер для расходов на вставку   
CREATE TRIGGER AfterInsertCosts
AFTER INSERT ON Costs
FOR EACH ROW
BEGIN
    UPDATE Projects
    SET Status = CASE
        WHEN (SELECT SUM(Amount) FROM Incomes WHERE Project_ID = NEW.Project_ID) >
             (SELECT SUM(Amount) FROM Costs WHERE Project_ID = NEW.Project_ID)
        THEN 'Прибыльный'
        ELSE 'Убыточный'
    END
    WHERE ID = NEW.Project_ID;
END; ''', language="sql")

        st.sidebar.subheader("Визуализация")

        visualization_topic = st.sidebar.selectbox("Сущность:",
                                                   ["Каналы", "Видео",
                                                    "Комментарии", "Проекты"],
                                                   index=None,
                                                   placeholder="Выберите сущность")

        if visualization_topic:
            visualize_data(visualization_topic)
    else:
        st.warning(
            "Вы не подключены к базе данных. Выберите и подключитесь к базе")

elif section == "Результаты":
    st.markdown(conclusion)
