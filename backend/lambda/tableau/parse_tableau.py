import pandas as pd
import sqlparse
import xml.etree.ElementTree as ET
from sqlparse.tokens import Keyword
from sql_metadata import Parser
from utils.authentication import authenticate_tableau, Environment
from utils.helpers import *


def parse_tableau():
    # Tableau Authentication
    AUTHENTICATION, SERVER = authenticate_tableau(Environment.PROD)

    # Initialize dictionaries to store outputs
    metadata_dict = dict()
    parsed_data_source_dict = dict()
    connections_list = dict()

    # Log in to Tableau Server and query all Data Sources on Server
    with SERVER.auth.sign_in_with_personal_access_token(AUTHENTICATION):
        all_data_sources, pagination_item = SERVER.datasources.get()
        print('There are {} data sources on site...'.format(pagination_item.total_available))

        # Get initial metadata about Datasources on Server
        for datasource in all_data_sources:

            # Populate connection information about each datasource
            SERVER.datasources.populate_connections(datasource)

            for connection in datasource.connections:
                log('[DS] datasource.name', datasource.name)

                connection_dict = {
                    'datasource_id': datasource.id,
                    'datasource_name': datasource.name,
                    'datasource_type': datasource.datasource_type,
                    'connection_id': connection.id
                }

                connections_list[datasource.id] = connection_dict

        # Download each Datasource that is NOT a text file, extract the .tds files, and return paths to each .tds.
        datasource_path_dict = download_data_sources(SERVER, all_data_sources)

        # Parse each data source file for its Custom SQL and store in dictionary
        for ds_id, ds_path in datasource_path_dict.items():
            with open(ds_path) as f:
                tree = ET.parse(f)
            root = tree.getroot()

            # Get human-readable name of datasource
            ds_name = connections_list.get(ds_id).get('datasource_name')

            # Connection and Relation properties of data source xml
            connections = [c for c in root.iter('connection')]
            relations = [r for r in root.iter('relation')]

            #
            # NOTE: It is important to collect the tables and columns from the
            # XML elements as well as the inner SQL text of Tableau data sources.
            #
            # Not all tables are defined in Custom or Initial SQL.
            #
            # Once connected to a database in Tableau, users can drag and drop
            # tables onto the data pane (where the Tableau Data Model is defined in UI).
            #

            # These are the table and field names as defined in the Tableau Data Model.
            tables = get_tables_from_xml(relations)
            columns = get_columns_from_xml(root)

            # These are the table and field names as defined in Initial SQL
            initial_sql = get_initial_sql(connections)
            parsed_initial_sql = parse_initial_sql(initial_sql)
            str_fmt_initial_sql = str_fmt_sql(initial_sql)

            # These are the table and field names as defined in Custom SQL.
            custom_sql = get_custom_sql(relations)
            parsed_custom_sql = parse_custom_sql(custom_sql)
            str_fmt_custom_sql = str_fmt_sql(custom_sql)

            # Combine parsed XML (Tableau Data Model), Initial SQL, and Custom SQL tables and columns
            all_tables = list(set(
                parsed_initial_sql.get('source_tables', list())
                + parsed_custom_sql.get('source_tables', list())
                + tables
            ))

            all_columns = list(set(
                parsed_initial_sql.get('field_names', list())
                + parsed_custom_sql.get('field_names', list())
                + list(columns.keys())
            ))

            # Update each output dictionaries with their respective parsed information
            parsed_data_source_dict[ds_id] = {
                'datasource_id': ds_id,
                'datasource_name': ds_name,
                'source_table_names_list': all_tables,
                'source_field_names_list': all_columns,
                'initial_sql': str_fmt_initial_sql,
                'custom_sql': str_fmt_custom_sql,
            }

    print('PARSED DATA SOURCE DICT...')

    #
    # OUTPUT: CSV
    #
    data = list()
    for _, ds in parsed_data_source_dict.items():
        data.append(list(ds.values()))

    columns = [
        'datasource_id',
        'datasource_name',
        'source_table_names_list',
        'source_field_names_list',
        'initial_sql',
        'custom_sql',
    ]

    df = pd.DataFrame(data, columns=columns)
    df.to_csv('./tableau_datasource_metadata.csv', encoding='utf-8', index=False)
    print(df)

    #
    # OUTPUT: JSON
    #
    # with open('./tableau_datasource_metadata.json', 'w') as f:
    #     json.dump(parsed_data_source_dict, f)

    #
    # OUTPUT: DynamoDB
    #
    # write_to_dynamodb(record=parsed_data_source_dict, pk='pk')

    # Remove all data source xml files from temporary directory
    delete_tmp_files_of_type('xml', '../tableau/tmp')

    return parsed_data_source_dict


def download_data_sources(tableau_server, datasources):
    # Store path to each file
    file_path_dict = dict()

    for datasource in datasources:
        # Filter out text files
        text_filters = ('hyper', 'webdata-direct', 'textscan')
        if datasource.datasource_type not in text_filters:

            # Download Packaged Tableau Datasource file (.tdsx)
            zipped_ds_path = tableau_server.datasources.download(
                datasource.id,
                filepath='../tableau/tmp',
                include_extract=False
            )

            # Extract Tableau Datasource file (.tds)
            unzipped_ds_path = unzip_packaged_tableau_file(
                zipped_file=zipped_ds_path,
                output_file_type='tds',
                output_dir='../tableau/tmp',
                obj_name=datasource.name
            )

            # Convert .tds to .xml
            xml_path = convert_tableau_file_to_xml(unzipped_ds_path)

            # Add path to dict
            file_path_dict[datasource.id] = xml_path

    # Remove all .tdsx from temporary directory
    delete_tmp_files_of_type('tdsx', '../tableau/tmp')

    return file_path_dict


def get_tables_from_xml(tables_xml):
    # Get table names
    tables_list = list()
    for relation in tables_xml:
        if relation.attrib.get('type') == 'table':
            cleaned_table_name = strip_brackets(
                relation.attrib.get('table')
            )
            if 'Extract' not in cleaned_table_name:
                tables_list.append(cleaned_table_name)

    # log('tables_list', tables_list)
    return tables_list


def get_columns_from_xml(root_xml):
    columns_dict = dict()

    # Get columns defined as map local-remote columns as key-value pairs
    cols_xml = root_xml.iter('cols')
    for c in cols_xml:
        for m in c.findall('map'):
            name = strip_brackets(m.attrib.get('key'))
            remote = strip_brackets(m.attrib.get('value'))

            columns_dict[remote] = {
                'local_column': name,
                'remote_column': remote
            }

    # Get Column names, aliases, and calculations
    columns_xml = [c for c in root_xml.iter('column')]
    for col in columns_xml:
        name = strip_brackets(
            col.attrib.get('name')
        )
        alias = col.attrib.get('caption')
        # Create human readable column name for Tableau calculated fields
        if alias:
            if 'Calculation_' in name:
                name = str('[Calculated Field] ' + alias)

        datatype = col.attrib.get('datatype')
        formula = None
        for calc in col.iter('calculation'):
            formula = calc.attrib.get('formula')

        columns_dict[name] = dict()
        if alias:
            columns_dict[name]['alias'] = alias

        if datatype:
            columns_dict[name]['datatype'] = datatype

        if formula:
            columns_dict[name]['calculation'] = formula

    # Remove auto generated "Number of Records" field from dictionary
    columns_dict.pop('Number of Records', None)

    # log('columns_dict', columns_dict)
    return columns_dict


def get_initial_sql(initial_sql_xml):
    initial_sql_list = list()
    for conn in initial_sql_xml:
        # Store Initial SQL query
        if bool(conn.attrib.get('one-time-sql')):
            # Format Custom SQL
            formatted_sql = sqlparse.format(
                conn.attrib.get('one-time-sql'),
                reindent=True,
                strip_comments=True,
                keyword_case='upper',
                comma_first=True
            )
            initial_sql_list.append(formatted_sql)

    return initial_sql_list


def get_custom_sql(custom_sql_xml):
    # Get Custom SQL
    custom_sql_list = list()
    for sql_item in custom_sql_xml:
        if sql_item.attrib.get('type') == 'text':
            # Format Custom SQL
            formatted_sql = sqlparse.format(
                sql_item.text,
                reindent=True,
                strip_comments=True,
                keyword_case='upper',
                comma_first=True
            )
            custom_sql_list.append(formatted_sql)

    return custom_sql_list


def parse_custom_sql(custom_sql_list):
    # Parse SQL
    table_metadata_dict = dict()
    for q in custom_sql_list:
        try:
            #
            # Table Names
            #
            table_names = [t for t in Parser(q).tables]
            # log('table_names', table_names)

            table_aliases_dict = Parser(q).tables_aliases
            # log('table_aliases_dict', table_aliases_dict)

            #
            # Column Names
            #
            columns = [c for c in Parser(q).columns]
            # log('columns', columns)

            column_aliases_dict = Parser(q).columns_aliases
            # log('column_aliases_dict', column_aliases_dict)

        except ValueError:
            continue

        except KeyError:
            continue

        #
        # Store Output
        #
        table_metadata_dict = {
            'source_tables': table_names,
            'source_table_aliases': table_aliases_dict,
            'field_names': columns,
            'field_name_aliases': column_aliases_dict
        }

    return table_metadata_dict


def parse_initial_sql(initial_sql_list):
    # Parse SQL
    table_metadata_dict = dict()
    for get_q in initial_sql_list:
        # If there is no SELECT statement, we can assume this is a TEMP TABLE and disregard
        if 'SELECT' in get_q:
            f_token = sqlparse.parse(get_q)[0].token_first()
            # Get the SELECT subquery of a statement that includes parser-breaking DML
            if not f_token.match(Keyword.DML, 'SELECT'):
                q = handle_unsupported_query_types(get_q)
            else:
                q = get_q

            try:
                #
                # Table Names
                #
                # log('pre table_names', q)
                table_names = [t for t in Parser(q).tables]
                # log('post table_names', table_names)

                table_aliases_dict = Parser(q).tables_aliases
                # log('table_aliases_dict', table_aliases_dict)

                #
                # Column Names
                #
                columns = [c for c in Parser(q).columns]
                # log('columns', columns)

                column_aliases_dict = Parser(q).columns_aliases
                # log('column_aliases_dict', column_aliases_dict)

            except ValueError:
                continue

            except KeyError:
                continue

            #
            # Store Output
            #
            table_metadata_dict = {
                'source_tables': table_names,
                'source_table_aliases': table_aliases_dict,
                'field_names': columns,
                'field_name_aliases': column_aliases_dict
            }

    return table_metadata_dict


def handle_unsupported_query_types(unsupported_sql):
    # Drop lines containing unsupported commands
    unsupported_commands = ['CREATE', 'DROP', 'TEMP']
    cleaned_query_split_lines = list()
    for line in unsupported_sql.splitlines():
        if not any(cmd in line for cmd in unsupported_commands):
            cleaned_query_split_lines.append(line)
        else:
            if 'SELECT' in line:
                cleaned_query_split_lines.append(
                    str('SELECT{}'.format(line.split("SELECT", maxsplit=1)[1]))
                )
    cleaned_query_string = '\n'.join(cleaned_query_split_lines)

    # Format Custom SQL
    formatted_queries = sqlparse.format(
        cleaned_query_string,
        reindent=True,
        strip_comments=True,
        keyword_case='upper',
        comma_first=True
    )

    # Parse out leftover parenthesis
    if 'SELECT' in formatted_queries:
        if not formatted_queries.count('SELECT') > 1:
            sep = '(SELECT'
            select_split = formatted_queries.split(sep, maxsplit=1)[1]
            stmt = f'SELECT{select_split}'
            split_stmt = stmt.replace(');', ';')
            validated_query = validate_parenthesis(split_stmt)
            # log('validated_query', validated_query)
            return validated_query
        else:
            print('Initial SQL has multiple SELECT Statements. Requires separate parsing...')
            return ''
    else:
        print('No remote database referenced in Initial SQL.')


def validate_parenthesis(query_string):
    lp = '('
    rp = ')'
    if not query_string.count(lp) == query_string.count(rp):
        equal_parenthesis = None
        if query_string.count(lp) < query_string.count(rp):
            equal_parenthesis = query_string.rsplit(rp, maxsplit=1)[0]
        if query_string.count(lp) > query_string.count(rp):
            equal_parenthesis = query_string.split(lp, maxsplit=1)[1]
        return validate_parenthesis(equal_parenthesis)
    else:
        return query_string


def str_fmt_sql(sql):
    new_line = '''
    '''
    sql_str_list = [
        q.replace('\n', new_line)
        for q in sql
    ]

    return sql_str_list


if __name__ == "__main__":
    parse_tableau()
