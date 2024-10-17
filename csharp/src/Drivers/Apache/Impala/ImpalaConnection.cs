/*
* Licensed to the Apache Software Foundation (ASF) under one or more
* contributor license agreements.  See the NOTICE file distributed with
* this work for additional information regarding copyright ownership.
* The ASF licenses this file to You under the Apache License, Version 2.0
* (the "License"); you may not use this file except in compliance with
* the License.  You may obtain a copy of the License at
*
*    http://www.apache.org/licenses/LICENSE-2.0
*
* Unless required by applicable law or agreed to in writing, software
* distributed under the License is distributed on an "AS IS" BASIS,
* WITHOUT WARRANTIES OR CONDITIONS OF ANY KIND, either express or implied.
* See the License for the specific language governing permissions and
* limitations under the License.
*/

using System;
using System.Collections.Generic;
using System.Diagnostics;
using System.Linq;
using System.Reflection;
using System.Text;
using System.Text.RegularExpressions;
using System.Threading;
using System.Threading.Tasks;
using Apache.Arrow.Adbc.Drivers.Apache.Hive2;
using Apache.Arrow.Adbc.Drivers.Apache.Thrift;
using Apache.Arrow.Adbc.Extensions;
using Apache.Arrow.Ipc;
using Apache.Arrow.Types;
using Apache.Hive.Service.Rpc.Thrift;
using Thrift;
using Thrift.Protocol;
using Thrift.Transport;

namespace Apache.Arrow.Adbc.Drivers.Apache.Impala
{
    internal abstract class ImpalaConnection : HiveServer2Connection
    {
        internal static readonly string s_userAgent = $"{InfoDriverName.Replace(" ", "")}/{ProductVersionDefault}";

        const string ProductVersionDefault = "1.0.0";
        const string InfoDriverName = "ADBC Impala Driver";
        const string InfoDriverArrowVersion = "1.0.0";
        const bool InfoVendorSql = true;
        const string ColumnDef = "COLUMN_DEF";
        const string ColumnName = "COLUMN_NAME";
        const string DataType = "DATA_TYPE";
        const string IsAutoIncrement = "IS_AUTO_INCREMENT";
        const string IsNullable = "IS_NULLABLE";
        const string OrdinalPosition = "ORDINAL_POSITION";
        const string TableCat = "TABLE_CAT";
        const string TableCatalog = "TABLE_CATALOG";
        const string TableName = "TABLE_NAME";
        const string TableSchem = "TABLE_SCHEM";
        const string TableType = "TABLE_TYPE";
        const string TypeName = "TYPE_NAME";
        const string Nullable = "NULLABLE";
        private readonly Lazy<string> _productVersion;


        /*
        // https://impala.apache.org/docs/build/html/topics/impala_ports.html
        // https://impala.apache.org/docs/build/html/topics/impala_client.html
        private const int DefaultSocketTransportPort = 21050;
        private const int DefaultHttpTransportPort = 28000;
        */

        internal ImpalaConnection(IReadOnlyDictionary<string, string> properties)
            : base(properties)
        {
            ValidateProperties();
            _productVersion = new Lazy<string>(() => GetProductVersion(), LazyThreadSafetyMode.PublicationOnly);
        }

        private void ValidateProperties()
        {
            ValidateAuthentication();
            ValidateConnection();
            ValidateOptions();
        }

        private static string GetProductVersion()
        {
            FileVersionInfo fileVersionInfo = FileVersionInfo.GetVersionInfo(Assembly.GetExecutingAssembly().Location);
            return fileVersionInfo.ProductVersion ?? ProductVersionDefault;
        }

        public override AdbcStatement CreateStatement()
        {
            return new ImpalaStatement(this);
        }

        protected static Uri GetBaseAddress(string? uri, string? hostName, string? path, string? port)
        {
            // Uri property takes precedent.
            if (!string.IsNullOrWhiteSpace(uri))
            {
                var uriValue = new Uri(uri);
                if (uriValue.Scheme != Uri.UriSchemeHttp && uriValue.Scheme != Uri.UriSchemeHttps)
                    throw new ArgumentOutOfRangeException(
                        AdbcOptions.Uri,
                        uri,
                        $"Unsupported scheme '{uriValue.Scheme}'");
                return uriValue;
            }

            bool isPortSet = !string.IsNullOrEmpty(port);
            bool isValidPortNumber = int.TryParse(port, out int portNumber) && portNumber > 0;
            bool isDefaultHttpsPort = !isPortSet || (isValidPortNumber && portNumber == 443);
            string uriScheme = isDefaultHttpsPort ? Uri.UriSchemeHttps : Uri.UriSchemeHttp;
            int uriPort;
            if (!isPortSet)
                uriPort = -1;
            else if (isValidPortNumber)
                uriPort = portNumber;
            else
                throw new ArgumentOutOfRangeException(nameof(port), portNumber, $"Port number is not in a valid range.");

            Uri baseAddress = new UriBuilder(uriScheme, hostName, uriPort, path).Uri;
            return baseAddress;
        }

        public override IArrowArrayStream GetObjects(GetObjectsDepth depth, string? catalogPattern, string? dbSchemaPattern, string? tableNamePattern, IReadOnlyList<string>? tableTypes, string? columnNamePattern)
        {
            Trace.TraceError($"getting objects with depth={depth.ToString()}, catalog = {catalogPattern}, dbschema = {dbSchemaPattern}, tablename = {tableNamePattern}");

            Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>> catalogMap = new Dictionary<string, Dictionary<string, Dictionary<string, TableInfo>>>();
            if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Catalogs)
            {
                TGetCatalogsReq getCatalogsReq = new TGetCatalogsReq(SessionHandle);

                TGetCatalogsResp getCatalogsResp = Client.GetCatalogs(getCatalogsReq).Result;
                if (getCatalogsResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new Exception(getCatalogsResp.Status.ErrorMessage);
                }
                var catalogsMetadata = GetResultSetMetadataAsync(getCatalogsResp).Result;
                IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(catalogsMetadata.Schema.Columns);

                string catalogRegexp = PatternToRegEx(catalogPattern);
                TRowSet rowSet = GetRowSetAsync(getCatalogsResp).Result;
                IReadOnlyList<string> list = rowSet.Columns[columnMap[TableCat]].StringVal.Values;
                for (int i = 0; i < list.Count; i++)
                {
                    string col = list[i];
                    string catalog = col;

                    if (Regex.IsMatch(catalog, catalogRegexp, RegexOptions.IgnoreCase))
                    {
                        catalogMap.Add(catalog, new Dictionary<string, Dictionary<string, TableInfo>>());
                    }
                }
                // Handle the case where server does not support 'catalog' in the namespace.
                if (list.Count == 0 && string.IsNullOrEmpty(catalogPattern))
                {
                    catalogMap.Add(string.Empty, []);
                }
            }

            if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.DbSchemas)
            {
                TGetSchemasReq getSchemasReq = new TGetSchemasReq(SessionHandle);
                getSchemasReq.CatalogName = catalogPattern;
                getSchemasReq.SchemaName = dbSchemaPattern;

                TGetSchemasResp getSchemasResp = Client.GetSchemas(getSchemasReq).Result;
                if (getSchemasResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new Exception(getSchemasResp.Status.ErrorMessage);
                }

                TGetResultSetMetadataResp schemaMetadata = GetResultSetMetadataAsync(getSchemasResp).Result;
                IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(schemaMetadata.Schema.Columns);
                TRowSet rowSet = GetRowSetAsync(getSchemasResp).Result;

                IReadOnlyList<string> catalogList = rowSet.Columns[columnMap[TableCatalog]].StringVal.Values;
                IReadOnlyList<string> schemaList = rowSet.Columns[columnMap[TableSchem]].StringVal.Values;

                for (int i = 0; i < catalogList.Count; i++)
                {
                    string catalog = catalogList[i];
                    string schemaDb = schemaList[i];
                    // It seems Spark sometimes returns empty string for catalog on some schema (temporary tables).
                    catalogMap.GetValueOrDefault(catalog)?.Add(schemaDb, new Dictionary<string, TableInfo>());
                }
            }

            if (depth == GetObjectsDepth.All || depth >= GetObjectsDepth.Tables)
            {
                TGetTablesReq getTablesReq = new TGetTablesReq(SessionHandle);
                getTablesReq.CatalogName = catalogPattern;
                getTablesReq.SchemaName = dbSchemaPattern;
                getTablesReq.TableName = tableNamePattern;

                TGetTablesResp getTablesResp = Client.GetTables(getTablesReq).Result;
                if (getTablesResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new Exception(getTablesResp.Status.ErrorMessage);
                }

                TGetResultSetMetadataResp tableMetadata = GetResultSetMetadataAsync(getTablesResp).Result;
                IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(tableMetadata.Schema.Columns);
                TRowSet rowSet = GetRowSetAsync(getTablesResp).Result;

                IReadOnlyList<string> catalogList = rowSet.Columns[columnMap[TableCat]].StringVal.Values;
                IReadOnlyList<string> schemaList = rowSet.Columns[columnMap[TableSchem]].StringVal.Values;
                IReadOnlyList<string> tableList = rowSet.Columns[columnMap[TableName]].StringVal.Values;
                IReadOnlyList<string> tableTypeList = rowSet.Columns[columnMap[TableType]].StringVal.Values;

                for (int i = 0; i < catalogList.Count; i++)
                {
                    string catalog = catalogList[i];
                    string schemaDb = schemaList[i];
                    string tableName = tableList[i];
                    string tableType = tableTypeList[i];
                    TableInfo tableInfo = new(tableType);
                    catalogMap.GetValueOrDefault(catalog)?.GetValueOrDefault(schemaDb)?.Add(tableName, tableInfo);
                }
            }

            if (depth == GetObjectsDepth.All)
            {
                TGetColumnsReq columnsReq = new TGetColumnsReq(SessionHandle);
                columnsReq.CatalogName = catalogPattern;
                columnsReq.SchemaName = dbSchemaPattern;
                columnsReq.TableName = tableNamePattern;

                if (!string.IsNullOrEmpty(columnNamePattern))
                    columnsReq.ColumnName = columnNamePattern;

                var columnsResponse = Client.GetColumns(columnsReq).Result;
                if (columnsResponse.Status.StatusCode == TStatusCode.ERROR_STATUS)
                {
                    throw new Exception(columnsResponse.Status.ErrorMessage);
                }

                TGetResultSetMetadataResp columnsMetadata = GetResultSetMetadataAsync(columnsResponse).Result;
                IReadOnlyDictionary<string, int> columnMap = GetColumnIndexMap(columnsMetadata.Schema.Columns);
                TRowSet rowSet = GetRowSetAsync(columnsResponse).Result;

                IReadOnlyList<string> catalogList = rowSet.Columns[columnMap[TableCat]].StringVal.Values;
                IReadOnlyList<string> schemaList = rowSet.Columns[columnMap[TableSchem]].StringVal.Values;
                IReadOnlyList<string> tableList = rowSet.Columns[columnMap[TableName]].StringVal.Values;
                IReadOnlyList<string> columnNameList = rowSet.Columns[columnMap[ColumnName]].StringVal.Values;
                ReadOnlySpan<int> columnTypeList = rowSet.Columns[columnMap[DataType]].I32Val.Values.Values;
                IReadOnlyList<string> typeNameList = rowSet.Columns[columnMap[TypeName]].StringVal.Values;
                ReadOnlySpan<int> nullableList = rowSet.Columns[columnMap[Nullable]].I32Val.Values.Values;
                IReadOnlyList<string> columnDefaultList = rowSet.Columns[columnMap[ColumnDef]].StringVal.Values;
                ReadOnlySpan<int> ordinalPosList = rowSet.Columns[columnMap[OrdinalPosition]].I32Val.Values.Values;
                IReadOnlyList<string> isNullableList = rowSet.Columns[columnMap[IsNullable]].StringVal.Values;
                IReadOnlyList<string> isAutoIncrementList = rowSet.Columns[columnMap[IsAutoIncrement]].StringVal.Values;

                for (int i = 0; i < catalogList.Count; i++)
                {
                    // For systems that don't support 'catalog' in the namespace
                    string catalog = catalogList[i] ?? string.Empty;
                    string schemaDb = schemaList[i];
                    string tableName = tableList[i];
                    string columnName = columnNameList[i];
                    short colType = (short)columnTypeList[i];
                    string typeName = typeNameList[i];
                    short nullable = (short)nullableList[i];
                    string? isAutoIncrementString = isAutoIncrementList[i];
                    bool isAutoIncrement = (!string.IsNullOrEmpty(isAutoIncrementString) && (isAutoIncrementString.Equals("YES", StringComparison.InvariantCultureIgnoreCase) || isAutoIncrementString.Equals("TRUE", StringComparison.InvariantCultureIgnoreCase)));
                    string isNullable = isNullableList[i] ?? "YES";
                    string columnDefault = columnDefaultList[i] ?? "";
                    // Spark/Databricks reports ordinal index zero-indexed, instead of one-indexed
                    int ordinalPos = ordinalPosList[i] + 1;
                    TableInfo? tableInfo = catalogMap.GetValueOrDefault(catalog)?.GetValueOrDefault(schemaDb)?.GetValueOrDefault(tableName);
                    tableInfo?.ColumnName.Add(columnName);
                    tableInfo?.ColType.Add(colType);
                    tableInfo?.Nullable.Add(nullable);
                    tableInfo?.IsAutoIncrement.Add(isAutoIncrement);
                    tableInfo?.IsNullable.Add(isNullable);
                    tableInfo?.ColumnDefault.Add(columnDefault);
                    tableInfo?.OrdinalPosition.Add(ordinalPos);
                    SetPrecisionScaleAndTypeName(colType, typeName, tableInfo);
                }
            }

            StringArray.Builder catalogNameBuilder = new StringArray.Builder();
            List<IArrowArray?> catalogDbSchemasValues = new List<IArrowArray?>();

            foreach (KeyValuePair<string, Dictionary<string, Dictionary<string, TableInfo>>> catalogEntry in catalogMap)
            {
                catalogNameBuilder.Append(catalogEntry.Key);

                if (depth == GetObjectsDepth.Catalogs)
                {
                    catalogDbSchemasValues.Add(null);
                }
                else
                {
                    catalogDbSchemasValues.Add(GetDbSchemas(
                                depth, catalogEntry.Value));
                }
            }

            Schema schema = StandardSchemas.GetObjectsSchema;
            IReadOnlyList<IArrowArray> dataArrays = schema.Validate(
                new List<IArrowArray>
                {
                    catalogNameBuilder.Build(),
                    catalogDbSchemasValues.BuildListArrayForType(new StructType(StandardSchemas.DbSchemaSchema)),
                });

            return new ImpalaInfoArrowStream(schema, dataArrays);
        }

        private static IReadOnlyDictionary<string, int> GetColumnIndexMap(List<TColumnDesc> columns) => columns
            .Select(t => new { Index = t.Position, t.ColumnName })
            .ToDictionary(t => t.ColumnName, t => t.Index);

        private static string PatternToRegEx(string? pattern)
        {
            if (pattern == null)
                return ".*";

            StringBuilder builder = new StringBuilder("(?i)^");
            string convertedPattern = pattern.Replace("_", ".").Replace("%", ".*");
            builder.Append(convertedPattern);
            builder.Append('$');

            return builder.ToString();
        }

        private static StructArray GetDbSchemas(
            GetObjectsDepth depth,
            Dictionary<string, Dictionary<string, TableInfo>> schemaMap)
        {
            StringArray.Builder dbSchemaNameBuilder = new StringArray.Builder();
            List<IArrowArray?> dbSchemaTablesValues = new List<IArrowArray?>();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;


            foreach (KeyValuePair<string, Dictionary<string, TableInfo>> schemaEntry in schemaMap)
            {

                dbSchemaNameBuilder.Append(schemaEntry.Key);
                length++;
                nullBitmapBuffer.Append(true);

                if (depth == GetObjectsDepth.DbSchemas)
                {
                    dbSchemaTablesValues.Add(null);
                }
                else
                {
                    dbSchemaTablesValues.Add(GetTableSchemas(
                        depth, schemaEntry.Value));
                }

            }

            IReadOnlyList<Field> schema = StandardSchemas.DbSchemaSchema;
            IReadOnlyList<IArrowArray> dataArrays = schema.Validate(
                new List<IArrowArray>
                {
                    dbSchemaNameBuilder.Build(),
                    dbSchemaTablesValues.BuildListArrayForType(new StructType(StandardSchemas.TableSchema)),
                });

            return new StructArray(
                new StructType(schema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private static StructArray GetTableSchemas(
            GetObjectsDepth depth,
            Dictionary<string, TableInfo> tableMap)
        {
            StringArray.Builder tableNameBuilder = new StringArray.Builder();
            StringArray.Builder tableTypeBuilder = new StringArray.Builder();
            List<IArrowArray?> tableColumnsValues = new List<IArrowArray?>();
            List<IArrowArray?> tableConstraintsValues = new List<IArrowArray?>();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;


            foreach (KeyValuePair<string, TableInfo> tableEntry in tableMap)
            {
                tableNameBuilder.Append(tableEntry.Key);
                tableTypeBuilder.Append(tableEntry.Value.Type);
                nullBitmapBuffer.Append(true);
                length++;


                tableConstraintsValues.Add(null);


                if (depth == GetObjectsDepth.Tables)
                {
                    tableColumnsValues.Add(null);
                }
                else
                {
                    tableColumnsValues.Add(GetColumnSchema(tableEntry.Value));
                }
            }


            IReadOnlyList<Field> schema = StandardSchemas.TableSchema;
            IReadOnlyList<IArrowArray> dataArrays = schema.Validate(
                new List<IArrowArray>
                {
                    tableNameBuilder.Build(),
                    tableTypeBuilder.Build(),
                    tableColumnsValues.BuildListArrayForType(new StructType(StandardSchemas.ColumnSchema)),
                    tableConstraintsValues.BuildListArrayForType( new StructType(StandardSchemas.ConstraintSchema))
                });

            return new StructArray(
                new StructType(schema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private static StructArray GetColumnSchema(TableInfo tableInfo)
        {
            StringArray.Builder columnNameBuilder = new StringArray.Builder();
            Int32Array.Builder ordinalPositionBuilder = new Int32Array.Builder();
            StringArray.Builder remarksBuilder = new StringArray.Builder();
            Int16Array.Builder xdbcDataTypeBuilder = new Int16Array.Builder();
            StringArray.Builder xdbcTypeNameBuilder = new StringArray.Builder();
            Int32Array.Builder xdbcColumnSizeBuilder = new Int32Array.Builder();
            Int16Array.Builder xdbcDecimalDigitsBuilder = new Int16Array.Builder();
            Int16Array.Builder xdbcNumPrecRadixBuilder = new Int16Array.Builder();
            Int16Array.Builder xdbcNullableBuilder = new Int16Array.Builder();
            StringArray.Builder xdbcColumnDefBuilder = new StringArray.Builder();
            Int16Array.Builder xdbcSqlDataTypeBuilder = new Int16Array.Builder();
            Int16Array.Builder xdbcDatetimeSubBuilder = new Int16Array.Builder();
            Int32Array.Builder xdbcCharOctetLengthBuilder = new Int32Array.Builder();
            StringArray.Builder xdbcIsNullableBuilder = new StringArray.Builder();
            StringArray.Builder xdbcScopeCatalogBuilder = new StringArray.Builder();
            StringArray.Builder xdbcScopeSchemaBuilder = new StringArray.Builder();
            StringArray.Builder xdbcScopeTableBuilder = new StringArray.Builder();
            BooleanArray.Builder xdbcIsAutoincrementBuilder = new BooleanArray.Builder();
            BooleanArray.Builder xdbcIsGeneratedcolumnBuilder = new BooleanArray.Builder();
            ArrowBuffer.BitmapBuilder nullBitmapBuffer = new ArrowBuffer.BitmapBuilder();
            int length = 0;


            for (int i = 0; i < tableInfo.ColumnName.Count; i++)
            {
                columnNameBuilder.Append(tableInfo.ColumnName[i]);
                ordinalPositionBuilder.Append(tableInfo.OrdinalPosition[i]);
                // Use the "remarks" field to store the original type name value
                remarksBuilder.Append(tableInfo.TypeName[i]);
                xdbcColumnSizeBuilder.Append(tableInfo.Precision[i]);
                xdbcDecimalDigitsBuilder.Append(tableInfo.Scale[i]);
                xdbcDataTypeBuilder.Append(tableInfo.ColType[i]);
                // Just the base type name without precision or scale clause
                xdbcTypeNameBuilder.Append(tableInfo.BaseTypeName[i]);
                xdbcNumPrecRadixBuilder.AppendNull();
                xdbcNullableBuilder.Append(tableInfo.Nullable[i]);
                xdbcColumnDefBuilder.Append(tableInfo.ColumnDefault[i]);
                xdbcSqlDataTypeBuilder.Append(tableInfo.ColType[i]);
                xdbcDatetimeSubBuilder.AppendNull();
                xdbcCharOctetLengthBuilder.AppendNull();
                xdbcIsNullableBuilder.Append(tableInfo.IsNullable[i]);
                xdbcScopeCatalogBuilder.AppendNull();
                xdbcScopeSchemaBuilder.AppendNull();
                xdbcScopeTableBuilder.AppendNull();
                xdbcIsAutoincrementBuilder.Append(tableInfo.IsAutoIncrement[i]);
                xdbcIsGeneratedcolumnBuilder.Append(true);
                nullBitmapBuffer.Append(true);
                length++;
            }

            IReadOnlyList<Field> schema = StandardSchemas.ColumnSchema;
            IReadOnlyList<IArrowArray> dataArrays = schema.Validate(
                new List<IArrowArray>
                {
                    columnNameBuilder.Build(),
                    ordinalPositionBuilder.Build(),
                    remarksBuilder.Build(),
                    xdbcDataTypeBuilder.Build(),
                    xdbcTypeNameBuilder.Build(),
                    xdbcColumnSizeBuilder.Build(),
                    xdbcDecimalDigitsBuilder.Build(),
                    xdbcNumPrecRadixBuilder.Build(),
                    xdbcNullableBuilder.Build(),
                    xdbcColumnDefBuilder.Build(),
                    xdbcSqlDataTypeBuilder.Build(),
                    xdbcDatetimeSubBuilder.Build(),
                    xdbcCharOctetLengthBuilder.Build(),
                    xdbcIsNullableBuilder.Build(),
                    xdbcScopeCatalogBuilder.Build(),
                    xdbcScopeSchemaBuilder.Build(),
                    xdbcScopeTableBuilder.Build(),
                    xdbcIsAutoincrementBuilder.Build(),
                    xdbcIsGeneratedcolumnBuilder.Build()
                });

            return new StructArray(
                new StructType(schema),
                length,
                dataArrays,
                nullBitmapBuffer.Build());
        }

        private static void SetPrecisionScaleAndTypeName(short colType, string typeName, TableInfo? tableInfo)
        {
            // Keep the original type name
            tableInfo?.TypeName.Add(typeName);
            switch (colType)
            {
                case (short)ColumnTypeId.DECIMAL:
                case (short)ColumnTypeId.NUMERIC:
                    {
                        SqlDecimalParserResult result = SqlTypeNameParser<SqlDecimalParserResult>.Parse(typeName, colType);
                        tableInfo?.Precision.Add(result.Precision);
                        tableInfo?.Scale.Add((short)result.Scale);
                        tableInfo?.BaseTypeName.Add(result.BaseTypeName);
                        break;
                    }

                case (short)ColumnTypeId.CHAR:
                case (short)ColumnTypeId.NCHAR:
                case (short)ColumnTypeId.VARCHAR:
                case (short)ColumnTypeId.LONGVARCHAR:
                case (short)ColumnTypeId.LONGNVARCHAR:
                case (short)ColumnTypeId.NVARCHAR:
                    {
                        SqlCharVarcharParserResult result = SqlTypeNameParser<SqlCharVarcharParserResult>.Parse(typeName, colType);
                        tableInfo?.Precision.Add(result.ColumnSize);
                        tableInfo?.Scale.Add(null);
                        tableInfo?.BaseTypeName.Add(result.BaseTypeName);
                        break;
                    }

                default:
                    {
                        SqlTypeNameParserResult result = SqlTypeNameParser<SqlTypeNameParserResult>.Parse(typeName, colType);
                        tableInfo?.Precision.Add(null);
                        tableInfo?.Scale.Add(null);
                        tableInfo?.BaseTypeName.Add(result.BaseTypeName);
                        break;
                    }
            }
        }

        protected Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetSchemasResp response) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client);
        protected Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetCatalogsResp response) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client);
        protected Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetColumnsResp response) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client);
        protected Task<TGetResultSetMetadataResp> GetResultSetMetadataAsync(TGetTablesResp response) =>
            GetResultSetMetadataAsync(response.OperationHandle, Client);
        protected Task<TRowSet> GetRowSetAsync(TGetTableTypesResp response) =>
            FetchResultsAsync(response.OperationHandle);
        protected Task<TRowSet> GetRowSetAsync(TGetColumnsResp response) =>
            FetchResultsAsync(response.OperationHandle);
        protected Task<TRowSet> GetRowSetAsync(TGetTablesResp response) =>
            FetchResultsAsync(response.OperationHandle);
        protected Task<TRowSet> GetRowSetAsync(TGetCatalogsResp response) =>
            FetchResultsAsync(response.OperationHandle);
        protected Task<TRowSet> GetRowSetAsync(TGetSchemasResp response) =>
            FetchResultsAsync(response.OperationHandle);

        protected async Task<TRowSet> FetchResultsAsync(TOperationHandle operationHandle, long batchSize = BatchSizeDefault, CancellationToken cancellationToken = default)
        {
            await PollForResponseAsync(operationHandle, Client, PollTimeMillisecondsDefault);
            TFetchResultsResp fetchResp = await FetchNextAsync(operationHandle, Client, batchSize, cancellationToken);
            if (fetchResp.Status.StatusCode == TStatusCode.ERROR_STATUS)
            {
                throw new HiveServer2Exception(fetchResp.Status.ErrorMessage)
                    .SetNativeError(fetchResp.Status.ErrorCode)
                    .SetSqlState(fetchResp.Status.SqlState);
            }
            return fetchResp.Results;
        }

        protected static async Task<TFetchResultsResp> FetchNextAsync(TOperationHandle operationHandle, TCLIService.IAsync client, long batchSize, CancellationToken cancellationToken = default)
        {
            TFetchResultsReq request = new(operationHandle, TFetchOrientation.FETCH_NEXT, batchSize);
            TFetchResultsResp response = await client.FetchResults(request, cancellationToken);
            return response;
        }

        public override IArrowArrayStream GetTableTypes()
        {
            throw new System.NotImplementedException();
        }

        public override Schema GetTableSchema(string? catalog, string? dbSchema, string tableName) => throw new System.NotImplementedException();

        internal override SchemaParser SchemaParser { get; } = new HiveServer2SchemaParser();

        //internal override IArrowArrayStream NewReader<T>(T statement, Schema schema, CancellationToken cancellationToken = default) => new HiveServer2Reader(statement, schema, dataTypeConversion: DataTypeConversion, cancellationToken);

        protected abstract void ValidateConnection();
        protected abstract void ValidateAuthentication();
        protected abstract void ValidateOptions();

        internal abstract ImpalaServerType ServerType { get; }

        internal struct TableInfo(string type)
        {
            public string Type { get; } = type;

            public List<string> ColumnName { get; } = new();

            public List<short> ColType { get; } = new();

            public List<string> BaseTypeName { get; } = new();

            public List<string> TypeName { get; } = new();

            public List<short> Nullable { get; } = new();

            public List<int?> Precision { get; } = new();

            public List<short?> Scale { get; } = new();

            public List<int> OrdinalPosition { get; } = new();

            public List<string> ColumnDefault { get; } = new();

            public List<string> IsNullable { get; } = new();

            public List<bool> IsAutoIncrement { get; } = new();
        }

        internal class ImpalaInfoArrowStream : IArrowArrayStream
        {
            private Schema schema;
            private RecordBatch? batch;

            public ImpalaInfoArrowStream(Schema schema, IReadOnlyList<IArrowArray> data)
            {
                this.schema = schema;
                this.batch = new RecordBatch(schema, data, data[0].Length);
            }

            public Schema Schema { get { return this.schema; } }

            public ValueTask<RecordBatch?> ReadNextRecordBatchAsync(CancellationToken cancellationToken = default)
            {
                RecordBatch? batch = this.batch;
                this.batch = null;
                return new ValueTask<RecordBatch?>(batch);
            }

            public void Dispose()
            {
                this.batch?.Dispose();
                this.batch = null;
            }
        }
    }
}
