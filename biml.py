from lxml import etree

class Biml(object):
    """
    object for creating biml for M3 extract
    """

    def __init__(self, table, columns, keys, filter_col):
        self.table = table
        self.columns = [col for col in sorted(columns, key=lambda col: col['ordinal_position'])]
        self.column_names = [col['column_name'] for col in self.columns]
        self.column_string = ', '.join(self.column_names)
        self.keys = keys
        self.filter = filter_col
        self.lookup_sql = 'SELECT ' + self.column_string + ' FROM extract.' + self.table
        self.non_key_columns = [col for col in self.column_names if col not in self.keys]

    def _variables(self):
        update_string = self.get_update_string(self.keys, self.column_names, self.table)

        variables = etree.Element('Variables')

        variables.append(self.create_variable('table', self.table))
        variables.append(self.create_variable('columns', self.column_string))
        variables.append(self.create_variable('extract_filter_field', self.filter))
        variables.append(self.create_variable('update_from_stage', update_string))

        return variables

    def _conversion(self):
        col_nodes = []
        for col in self.columns:
            col_nodes.append(self.create_data_conversion_column(col['column_name'],
                                                                'C_' + col['column_name'],
                                                                self.get_data_type(col)))

        columns = etree.Element('Columns')
        for node in col_nodes:
            columns.append(node)

        return columns

    def _lookup_inputs(self):
        return self.map_columns(self.keys, 'C_', '')

    def _lookup_outputs(self):
        return self.map_columns(self.non_key_columns, '', 'D_')

    def _filter_columns(self):
        expression_list = []
        for col in self.non_key_columns:
            expression_list.append(' (C_{col} != D_{col}) '.format(col=col))
        return '||'.join(expression_list)

    def _destination_columns(self):
        return self.map_columns(self.column_names, 'C_', '')

    @staticmethod
    def map_columns(columns, source_prefix, target_prefix):
        columns_node = etree.Element('Columns')
        for col in columns:
            columns_node.append(etree.Element('Column', {'SourceColumn': source_prefix + col,
                                                         'TargetColumn': target_prefix + col
                                                         }))
        return columns_node

    @staticmethod
    def get_data_type(col):
        data_definition = dict()
        if col['data_type'] == 'GRAPHIC':
            data_definition['DataType'] = 'StringFixedLength'
            data_definition['length'] = str(col['length'])
        elif col['data_type'] == 'DECIMAL' and col['numeric_scale'] == 0:
            int_length = col['length']
            if int_length == 1:
                data_definition['DataType'] = 'Byte'
            elif int_length > 1 and int_length <= 4:
                data_definition['DataType'] = 'Int16'
            elif int_length > 4 and int_length <= 8:
                data_definition['DataType'] = 'Int32'
            else:
                data_definition['DataType'] = 'Int64'
        elif col['data_type'] == 'DECIMAL':
            data_definition['DataType'] = 'Decimal'
            data_definition['precision'] = str(col['length'])
            data_definition['scale'] = str(col['numeric_scale'])
        return data_definition

    @staticmethod
    def create_variable(name, text):
        var = etree.Element('Variable', {'Name': name, 'DataType': 'String'})
        var.text = text
        return var

    @staticmethod
    def create_data_conversion_column(source, target, data_type):
        attributes = {'SourceColumn': source, 'TargetColumn': target}
        attributes.update(data_type)
        t = etree.Element('Column', attributes)
        return etree.Element('Column', attributes)

    @staticmethod
    def get_update_string(keys, columns, table):
        col_string = 'UPDATE dest SET '
        col_list = ['dest.{column} = upd.{column}'.format(column=column) for column in columns]
        col_string += ', '.join(col_list)

        key_string = ' FROM extract.{table} dest INNER JOIN stage.{table}_STAGE upd ON '.format(table=table)
        key_list = ['upd.{key} = dest.{key}'.format(key=key) for key in keys]
        key_string += ', '.join(key_list)

        return col_string + key_string

