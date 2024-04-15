from datetime import datetime, timedelta
from pyspark.sql.functions import col
from pyspark.sql.types import StructType, LongType, StructField
from pyspark.sql.functions import radians, sin, cos, sqrt, asin, col
from pyspark.sql import functions as F
from pyspark.sql import Window, DataFrame, SparkSession
from pyspark.sql import SparkSession
spark = SparkSession.builder.getOrCreate()
spark.conf.set("spark.sql.ansi.enabled", False)
spark.conf.set("spark.sql.repl.eagerEval.enabled", True)
#-----------------------------------------------------------------------------------------------------------------

def rename_col(df, index, new_name):
    cols = df.columns
    cols[index] = new_name
    df = df.toDF(*cols)
    return df

def find_index_col(df, col_name):
    index = 0
    indexes = []
    vezes = 1
    for coluna in df.columns:
        if coluna == col_name:
            indexes.append(index)
            vezes+=1
        index+=1
    return indexes

def looking_for_ambiguity(df):
    changed = False
    for coluna in df.columns:
        indexes = find_index_col(df, coluna)
        if len(indexes)>1:
            for i in indexes[1:]:
                new_df = rename_col(df ,i, f'to_drop_{i}').drop(f'to_drop_{i}')
                changed = True
    if changed:
        return new_df
    if not changef:
        return df


def sao_dataframes_iguais(df1, df2):
    if df1.schema != df2.schema:
        #raise ValueError("Os DataFrames devem ter o mesmo esquema.")
        print('Os dfs não tem o mesmo schema:')
        return False
    diff_df = df1.exceptAll(df2)
    print('MESMOS ELEMENTOS : ')
    return diff_df.count() == 0

def igualar_esquema(spark, df1:DataFrame, df2:DataFrame) -> DataFrame:
    """
    Iguala o esquema de dois DataFrames PySpark, garantindo que ambos tenham as mesmas colunas.
    
    Args:
    - spark: Sessão Spark.
    - df1: Primeiro DataFrame PySpark.
    - df2: Segundo DataFrame PySpark.
    
    Returns:
    - DataFrame df2 com o mesmo esquema que df1.
    """
    if len(df1.columns) != len(df2.columns):
        raise ValueError("Os DataFrames devem ter o mesmo número de colunas.")
    for coluna in df1.columns:
        if coluna in df2.columns:
            tipo_coluna_df1 = df1.schema[coluna].dataType
            tipo_coluna_df2 = df2.schema[coluna].dataType

            if tipo_coluna_df1 != tipo_coluna_df2:
                df2 = df2.withColumn(coluna, F.col(coluna).cast(tipo_coluna_df1))
            nullable_coluna_df1 = df1.schema[coluna].nullable
            nullable_coluna_df2 = df2.schema[coluna].nullable

            if nullable_coluna_df1 != nullable_coluna_df2:
                df2 = df2.withColumn(coluna, F.when(F.col(coluna).isNotNull(), F.col(coluna)).otherwise(F.lit(None).cast(tipo_coluna_df1)))
                df2 = df2.na.fill(0, [coluna])

    return df2


def mesmos_elementos(spark, spark_df1: DataFrame, spark_df2: DataFrame) -> bool:
    """
    Verifica se dois DataFrames Spark são equivalentes, após as seguintes operações:
    1. Igualar o esquema dos DataFrames Spark.
    2. Verificar se os DataFrames resultantes são iguais.
    
    Args:
    - spark: Sessão Spark.
    - spark_df1: DataFrame Spark.
    - spark_df2: DataFrame Spark.
    
    Returns:
    - True se os DataFrames são equivalentes, False caso contrário.
    """
    # 1. Igualar o esquema dos DataFrames Spark
    spark_df2 = igualar_esquema(spark, spark_df1, spark_df2)

    # 2. Verificar se os DataFrames Spark resultantes são iguais
    return sao_dataframes_iguais(spark_df1, spark_df2)

def mude_o_primeiro_valor(df_royal, valor_modificado, coluna):
    #valor_modificado = 10  # Novo valor desejado
    #coluna = "CD_COMP"
    df_modificado = df_royal.withColumn(coluna, F.when(F.col(coluna) == df_royal.select(coluna).first()[0], valor_modificado).otherwise(F.col(coluna)))
    return df_modificado


def equalize_rows(spark: SparkSession, spark_df1: DataFrame, spark_df2: DataFrame) -> DataFrame:
    """
    Função que iguala as linhas de dois DataFrames Spark.

    Argumentos:
    spark (pyspark.sql.SparkSession): Sessão Spark.
    spark_df1 (pyspark.sql.DataFrame): Primeiro DataFrame Spark.
    spark_df2 (pyspark.sql.DataFrame): Segundo DataFrame Spark.

    Retorna:
    pyspark.sql.DataFrame: DataFrame Spark com as linhas igualadas.
    """
    # Verificar se os objetos fornecidos são do tipo DataFrame Spark
    if not isinstance(spark_df1, DataFrame) or not isinstance(spark_df2, DataFrame):
        raise ValueError("Os objetos fornecidos devem ser do tipo DataFrame Spark.")

    # Verificar se os esquemas dos DataFrames são iguais
    if spark_df1.schema != spark_df2.schema:
        raise ValueError("Os DataFrames fornecidos devem ter o mesmo esquema.")

    # Identificar as colunas-chave para realizar a junção
    colunas_chave = [col.name for col in spark_df1.schema]

    # Realizar a junção dos DataFrames
    spark_df_equalized = spark_df1.join(spark_df2, on=colunas_chave, how='inner')

    return spark_df_equalized, spark_df2

def mesmo_numero_de_linhas_e_colunas(df1, df2) -> bool:
    """
    Verifica se dois DataFrames têm o mesmo número de linhas e colunas.
    
    Args:
    - df1: DataFrame Pandas ou DataFrame PySpark.
    - df2: DataFrame Pandas ou DataFrame PySpark.
    
    Returns:
    - True se os DataFrames tiverem o mesmo número de linhas e colunas, False caso contrário.
    
    Raises:
    - ValueError: Se o tipo de DataFrame não for suportado.
    """
#    if isinstance(df1, pd.DataFrame):
#        qtd_linhas1, qtd_colunas1 = df1.shape
    if isinstance(df1, DataFrame):
        qtd_linhas1 = df1.count()
        qtd_colunas1 = len(df1.columns)
    else:
        raise ValueError("Tipo de DataFrame não suportado para df1")

    # if isinstance(df2, pd.DataFrame):
        # qtd_linhas2, qtd_colunas2 = df2.shape
    if isinstance(df2, DataFrame):
        qtd_linhas2 = df2.count()
        qtd_colunas2 = len(df2.columns)
    else:
        raise ValueError("Tipo de DataFrame não suportado para df2")
    print('type(df1) = ',type(df1))
    if len(df1.columns)<50:
        print('df1.cols = ', df1.columns)
    print(f'qtd_colunas1 = {qtd_colunas1}')
    print(f'qtd_linhas1 = {qtd_linhas1}')
    #print('qtd_linhas1 : ',qtd_linhas1)
    #print('qtd_colunas1 : ',qtd_colunas1)
    print('-'*40)
    print('type(df2) = ',type(df2))
    if len(df2.columns)<50:
        print('df2.cols = ', df2.columns)#.tolist())
    print('qtd_colunas2 : ', qtd_colunas2)
    print('qtd_linhas2 : ', qtd_linhas2)
    columns_df1 = set(df1.columns)
    columns_df2 = set(df2.columns)
    sorted_columns_df1 = sorted(columns_df1)
    sorted_columns_df2 = sorted(columns_df2)
    columns_only_in_df2 = columns_df2 - columns_df1
    ordem_das_colunas = sorted_columns_df1 == sorted_columns_df2
    print('-'*40)
    print(f'Ordem das colunas iguais ? {ordem_das_colunas}')
    if not ordem_das_colunas:
        print(f'Existem {len(columns_only_in_df2)} colunas com nomes errados no df2')
        print(f'Colunas que estão apenas no df2 : {columns_only_in_df2}')
    print(f'Nomes de colunas iguais ? {columns_df1 == columns_df2}')
    print('-'*40)
    print(f'Diferença de n linhas = {qtd_linhas1 - qtd_linhas2}')
    print(f'Qtd de linhas iguais ? {qtd_linhas1 == qtd_linhas2}')
    
    return qtd_linhas1 == qtd_linhas2 and qtd_colunas1 == qtd_colunas2 and columns_df1 == columns_df2


def subtrair_colunas(df1, df2, coluna1, coluna2):
    if coluna1 not in df1.columns or coluna2 not in df2.columns:
        raise ValueError("Uma ou ambas as colunas especificadas não existem nos DataFrames fornecidos.")

    # Realiza a subtração e substitui valores nulos por 0
    resultado_df = df1.select(
        (col(coluna1) - col(coluna2)).alias("subtracao_" + coluna1 + "_" + coluna2)
    ).na.fill(0, subset=["subtracao_" + coluna1 + "_" + coluna2])

    return resultado_df

def equalize_subtrair_colunas(spark, df1, df2, coluna1, coluna2):
    df1, df2 = equalize_rows(spark, df1, df2)
    if coluna1 not in df1.columns or coluna2 not in df2.columns:
        raise ValueError("Uma ou ambas as colunas especificadas não existem nos DataFrames fornecidos.")

    # Realiza a subtração e substitui valores nulos por 0 
    resultado_df = df1.select(
        (col(coluna1) - col(coluna2)).alias("subtracao_" + coluna1 + "_" + coluna2)
    ).na.fill(0, subset=["subtracao_" + coluna1 + "_" + coluna2])

    return resultado_df

def schema_like_spark(df):
    schema_str = ""
    for col_name, dtype in df.dtypes.items():
        nullable = "true" if df[col_name].isnull().any() else "false"
        schema_str += f"|-- {col_name}: {str(dtype).replace('object', 'string')} (nullable = {nullable})\n"
    for line in schema_str.split("\n"):
        print(line)
    #return schema_str

def change_nullability(df:DataFrame, columns_to_change):
    """
    Altera a nullabilidade das colunas especificadas em um DataFrame Spark.
    
    Args:
    - df: DataFrame Spark
    - columns_to_change: Lista de nomes das colunas cuja nullabilidade será alterada
    
    Returns:
    - DataFrame Spark com a nullabilidade alterada nas colunas especificadas
    """
    #columns_to_change = df.columns
    # Define um novo esquema com nullabilidade alterada para as colunas especificadas
    new_schema = StructType([
        StructField(col_name, LongType(), nullable=(col_name not in columns_to_change))
        for col_name in df.schema.names
    ])
    
    # Cria um novo DataFrame com o mesmo conteúdo, mas com o novo esquema
    new_df = spark.createDataFrame(df.rdd, new_schema)
    
    return new_df

def ajustar_tipo_timestamp(df: DataFrame) -> DataFrame:
    for coluna in df.columns:
        # Verificar se a coluna é do tipo datetime64[ns]
        if df.schema[coluna].dataType == 'timestamp':
            df = df.withColumn(coluna, col(coluna).cast("timestamp"))
    return df

def search_value(df, search_value):
    filtered_df = b.filter(b["CD_PTOV"] == search_value)
    return filtered_df

def diff_dataframes_ss(df_spark1: DataFrame, df_spark2: DataFrame):
    """
    Função que retorna os itens únicos em cada DataFrame.

    Argumentos:
    df_spark1 (pyspark.sql.DataFrame): DataFrame do Spark.
    df_spark2 (pyspark.sql.DataFrame): DataFrame do Spark.

    Retorna:
    tuple: Uma tupla contendo duas listas, onde o primeiro elemento é a lista de itens únicos no primeiro DataFrame Spark
           e o segundo elemento é a lista de itens únicos no segundo DataFrame Spark.
    """
    # Verifica as linhas únicas em cada DataFrame
    unique_spark1 = df_spark1.dropDuplicates()
    unique_spark2 = df_spark2.dropDuplicates()

    # Converte os DataFrames do Spark para conjuntos de tuplas
    set_spark1 = set(map(tuple, unique_spark1.collect()))
    set_spark2 = set(map(tuple, unique_spark2.collect()))

    #set_spark1 = set(map(tuple, df_spark1.collect()))
    #set_spark2 = set(map(tuple, df_spark2.collect()))

    # Calcula a diferença entre os conjuntos
    unique_in_spark1 = set_spark1 - set_spark2
    unique_in_spark2 = set_spark2 - set_spark1

    # Retorna os itens únicos em cada DataFrame
    return list(unique_in_spark1), list(unique_in_spark2), set_spark1, set_spark2


def check_lists_equal(list1:list, list2:list) -> bool:
    """
    Função que verifica se duas listas são iguais.
    
    Argumentos:
    list1 (list): Primeira lista a ser comparada.
    list2 (list): Segunda lista a ser comparada.
    
    Retorna:
    bool: True se as listas forem iguais, False caso contrário.
    """
    # Verifica se as listas têm o mesmo comprimento
    if len(list1) != len(list2):
        return False
    
    # Verifica se cada elemento em uma lista está presente na outra lista
    for item in list1:
        if item not in list2:
            return False
    
    # Se nenhum problema foi encontrado, as listas são iguais
    return True



######################################## UTILS ######################################################################################

def fix_columns_names(df:DataFrame,problem:str, replacer:str) -> DataFrame:
    new_columns = [col.replace(problem, replacer) for col in df.columns]
    return df.toDF(*new_columns)

def renomear_colunas(df):
    new_columns = [col.replace('.', '') for col in df.columns]
    return df.toDF(*new_columns)

def renomear_colunas_percent(df):
    new_columns = [col.replace('%', '') for col in df.columns]
    return df.toDF(*new_columns)

def renomear_colunas_(df):
    new_columns = [col.replace(' ', '_') for col in df.columns]
    return df.toDF(*new_columns)

def renomear_lista_de_colunas(lista_strings):
    new_columns = [col.replace('.', '') for col in lista_strings]
    return new_columns

#-----------------------------------------------------------------------------------------------------------------------------
def filter_df_royal(df):
    three_years_ago = datetime.now() - timedelta(days=3 * 365 + 30)  # Aproximação de 3 anos em dias
    year_month_three_years_ago = three_years_ago.strftime("%Y%m")
    df_filtered = df.filter(df.NO_AM > year_month_three_years_ago)
    return df_filtered

def filter_df_venda_prod(df):
    three_years_ago = datetime.now() - timedelta(days=3 * 365 + 2 * 30)  # Aproximação de 3 anos em dias
    year_month_three_years_ago = three_years_ago.strftime("%Y%m")
    df_filtered = df.filter(df.NO_AM > year_month_three_years_ago)

    return df_filtered

def filter_df_venda_diaria(df):
    df2 = df.withColumn("DT_REF_date", col("DT_REF").cast("date"))

    # Obter a data atual e calcular a data de 3 anos atrás
    data_atual = spark.sql("SELECT CURRENT_DATE() as hoje").first()["hoje"]
    data_tres_anos_atras = data_atual - timedelta(days=3 * 365)

    # Filtrar apenas os últimos 3 anos
    df_venda_diaria_filtrado = df2.filter(col("DT_REF_date") >= data_tres_anos_atras)
    df_venda_diaria_filtrado = df_venda_diaria_filtrado.drop("DT_REF_date")
    return df_venda_diaria_filtrado


def drop_index_column(df: DataFrame) -> DataFrame:
    if "__index_level_0__" in df.columns:
        return df.drop("__index_level_0__")
    else:
        return df
    
def somar_meses(today, num_meses):
    ano = today.year + (num_meses // 12)
    mes = today.month + (num_meses % 12)
    if mes > 12:
        mes -= 12
        ano += 1
    return datetime(ano, mes, today.day)

def subtrair_meses(today, num_meses):
    ano = today.year - (num_meses // 12)
    mes = today.month - (num_meses % 12)
    if mes <= 0:
        mes += 12
        ano -= 1
    return datetime(ano, mes, 1)
#-----------------------------------------------------------------------------------------------------------------------------
def normalize(s):
    """
    Função para normalizar strings removendo caracteres especiais.
    """
    return s.normalize("NFKD").encode("ascii", "ignore").decode("utf-8")

def get_first_row(df, sortby_col, groupby_col, ascending=False):
    """
    Função que retorna primeira informação para cada registro na coluna 'groupby_col'.
    """
    window_spec = Window.partitionBy(groupby_col).orderBy(F.col(sortby_col).desc() if ascending else F.col(sortby_col).asc())
    return df.withColumn("rn", F.row_number().over(window_spec)).filter(F.col("rn") == 1).drop("rn")

def last_year_boundaries(data_final):
    """
    Função que retorna lista com limites do ano anterior,
    no formato NO_AM, dada uma data final.
    """
    noam_inicial = int((data_final - F.expr("INTERVAL 12 MONTH")).strftime("%Y%m"))
    noam_final = int(data_final.strftime("%Y%m"))
    return [noam_inicial, noam_final]

def date_to_noam(date) -> int:
    """
    Altera uma data para o formato NO_AM.
    """
    return int(date.strftime("%Y%m"))

def filter_database(database, boundaries):
    """
    Função que filtra base de dados conforme limites fornecidos.
    """
    return database.filter(F.col("NO_AM").between(*boundaries))

def remove_outlier(df, col, upper_only=False, return_outlier=False):
    """
    Remove os outliers do DataFrame 'df' na coluna especificada por 'col'.
    
    Argumentos:
        df (DataFrame): O DataFrame contendo os dados.
        col (str): O nome da coluna na qual os outliers serão identificados e removidos.
        upper_only (bool, opcional): Se True, remove apenas os outliers superiores. Caso contrário, remove todos os outliers. O padrão é False.
        return_outlier (bool, opcional): Se True, retorna um DataFrame contendo apenas os outliers removidos. Caso contrário, retorna o DataFrame sem os outliers. O padrão é False.
        
    Retorna:
        DataFrame: O DataFrame sem os outliers, se 'return_outlier' for False (padrão), ou um DataFrame contendo apenas os outliers removidos, se 'return_outlier' for True.
    """
    quantiles = df.stat.approxQuantile(col, [0.25, 0.75], 0.01)
    q1, q3 = quantiles[0], quantiles[1]
    iqr = q3 - q1
    lower_bound = q1 - 1.5 * iqr
    upper_bound = q3 + 1.5 * iqr

    if upper_only:
        df_wo_outlier = df.filter(F.col(col) < upper_bound)
        df_outlier = df.filter(F.col(col) >= upper_bound)
    else:
        df_wo_outlier = df.filter((F.col(col) > lower_bound) & (F.col(col) < upper_bound))
        df_outlier = df.filter((F.col(col) <= lower_bound) | (F.col(col) >= upper_bound))

    if return_outlier:
        return df_outlier
    else:
        return df_wo_outlier

def ultimo_dia_mes(date):
    proximo_mes = date.replace(day=28) + timedelta(days=4)  # Vá para o dia 28 para garantir que você está no mês correto
    return proximo_mes - timedelta(days=proximo_mes.day)

def generate_intervalos(today:None):
    """
    Generate time intervals for filtering databases used in feature creation for projections.
    """
    if today is None:
        today = datetime(2024, 3, 6).date().replace(day=1)
        # today = datetime.now()
    data_inicial_jet = subtrair_meses(today, 26)
    data_final_jet = ultimo_dia_mes(subtrair_meses(today, 2))
    #return data_final_jet, data_inicial_jet
    # Calculate the boundaries for the intervals
    # 3 Meses anteriores
    no_am_ini_3pre = date_to_noam(somar_meses(data_inicial_jet,9))
    no_am_fim_3pre = date_to_noam(subtrair_meses(data_final_jet, 12))
    # 12 Meses anteriores
    no_am_ini_12pre = date_to_noam(data_inicial_jet)
    no_am_fim_12pre = date_to_noam(subtrair_meses(data_final_jet,12))
    # 12 Meses posteriores
    no_am_ini_12pos = date_to_noam(somar_meses(data_inicial_jet,11))
    no_am_fim_12pos = date_to_noam(data_final_jet)
    # Intervalo Features Projeção Realista
    # 3 Meses anteriores
    no_am_ini_3m_proj = date_to_noam(subtrair_meses(data_final_jet,2))
    no_am_fim_12pos_2 = date_to_noam(subtrair_meses(data_final_jet,11))
    # Create the dictionary
    intervalos = {
        "no_am_fim_12pre": no_am_fim_12pre,
        "no_am_fim_12pos": no_am_fim_12pos,
        "intervalo_geral_realista": [no_am_ini_12pre, no_am_fim_12pos],
        "intervalo_12m_train_realista": [no_am_ini_12pre, no_am_fim_12pre],
        "intervalo_3m_train_realista": [no_am_ini_3pre, no_am_fim_3pre],
        "intervalo_12m_tgt_realista": [no_am_ini_12pos, no_am_fim_12pos],
        "intervalo_3m_proj_realista": [no_am_ini_3m_proj, no_am_fim_12pos],
        "intervalo_12m_proj_realista": [no_am_fim_12pos_2, no_am_fim_12pos]

        #"intervalo_12m_proj_realista": last_year_boundaries(no_am_fim_12pos)
    }

    return intervalos

def haversine_distance(lat1, lon1, lat2, lon2):
    """
    Calcula a distância haversine entre dois pontos geográficos em metros.
    """
    lat1, lon1, lat2, lon2 = map(radians, [lat1, lon1, lat2, lon2])
    dlat = lat2 - lat1
    dlon = lon2 - lon1
    a = sin(dlat / 2) ** 2 + cos(lat1) * cos(lat2) * sin(dlon / 2) ** 2
    c = 2 * asin(sqrt(a))
    r = 6371 * 1000  # Earth radius in meters
    return c * r

def encontra_vizinhos(df, lat_col, lon_col, chave_primaria, no_vizinhos=5, mantem_latlong=False):
    """
    Função que retorna N vizinhos mais próximos para todos os IDs únicos em um dataframe.
    """
    df = df.crossJoin(df.withColumnRenamed(chave_primaria, chave_primaria + "_VIZINHO")) \
           .filter(F.col(chave_primaria) != F.col(chave_primaria + "_VIZINHO"))

    lat_col_rad = lat_col + "_rad"
    lon_col_rad = lon_col + "_rad"
    df = df.withColumn(lat_col_rad, radians(F.col(lat_col))) \
           .withColumn(lon_col_rad, radians(F.col(lon_col))) \
           .withColumn(lat_col + "_VIZINHO_rad", radians(F.col(lat_col + "_VIZINHO"))) \
           .withColumn(lon_col + "_VIZINHO_rad", radians(F.col(lon_col + "_VIZINHO")))

    df = df.withColumn("DELTA_LAT", F.col(lat_col + "_VIZINHO_rad") - F.col(lat_col_rad)) \
           .withColumn("DELTA_LON", F.col(lon_col + "_VIZINHO_rad") - F.col(lon_col_rad))

    df = df.withColumn("DISTANCIA", haversine_distance(F.col(lat_col_rad), F.col(lon_col_rad), F.col(lat_col + "_VIZINHO_rad"), F.col(lon_col + "_VIZINHO_rad"))) \
           .orderBy("DISTANCIA").groupBy(chave_primaria).agg(F.collect_list(chave_primaria + "_VIZINHO").alias("VIZINHOS")) \
           .withColumn("VIZINHOS", F.slice(F.col("VIZINHOS"), 1, no_vizinhos))

    if not mantem_latlong:
        df = df.drop(*[lat_col + "_VIZINHO", lon_col + "_VIZINHO", lat_col_rad, lon_col_rad, lat_col + "_VIZINHO_rad", lon_col + "_VIZINHO_rad", "DELTA_LAT", "DELTA_LON"])

    return df
