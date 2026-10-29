import { useQuery } from '@tanstack/preact-query'
import { Plug } from 'lucide-preact'
import { useLocation } from 'preact-iso'

import { cluster, type Cluster } from '../api/cluster'
import { REFRESH_MS } from '../api/query'
import { Code } from '../components/code'
import { Heading } from '../components/heading'
import { Input } from '../components/input'
import { Loading } from '../components/loading'
import { Notice } from '../components/notice'
import { Toolbar } from '../components/toolbar'
import { View } from '../components/view'
import { withQuery } from '../location'

import './connect.css'

export default function Connect() {
  const { path, query, route } = useLocation()
  const state = useQuery({ ...cluster(), refetchInterval: REFRESH_MS })
  const table = query.table || '<table>'
  const trail = [{ label: 'Connect' }]

  if (!state.data) {
    return (
      <View trail={trail}>
        <Loading error={state.error} />
      </View>
    )
  }

  const data = state.data
  return (
    <View trail={trail}>
      <Heading title="Connect an engine" icon={Plug} />
      {state.error && <Notice kind="danger">{state.error.message}</Notice>}
      <div class="connect">
        <Code label="Catalog URL" text={data.catalogUrl} />
        <Code label="Namespace" text={data.namespace} />
        <Code label="Warehouse" text={data.warehouse} />
        <Code label="S3 endpoint" text={data.s3Endpoint} />
      </div>
      <Toolbar>
        <Input
          prefix="Table"
          placeholder="orders"
          value={query.table ?? ''}
          onInput={(event) => route(withQuery(path, query, { table: event.currentTarget.value }), true)}
        />
      </Toolbar>
      <div class="connect">
        <Code label="DuckDB" text={duckdb(data, table)} />
        <Code label="PyIceberg" text={pyiceberg(data, table)} />
        <Code label="Spark SQL" text={spark(data, table)} />
        <Code label="Trino catalog properties" text={trino(data)} />
      </div>
    </View>
  )
}

function host(url: string): string {
  return url.replace(/^https?:\/\//, '')
}

function duckdb(data: Cluster, table: string): string {
  return [
    'INSTALL iceberg; LOAD iceberg; INSTALL httpfs; LOAD httpfs;',
    `CREATE SECRET mink (TYPE S3, KEY_ID 'mink', SECRET 'mink', REGION 'us-east-1', ENDPOINT '${host(data.s3Endpoint)}', URL_STYLE 'path', USE_SSL ${data.s3Endpoint.startsWith('https') ? 'true' : 'false'});`,
    `ATTACH 'warehouse' AS mink (TYPE iceberg, ENDPOINT '${data.catalogUrl}', AUTHORIZATION_TYPE 'none');`,
    `SELECT count(*) FROM mink.${data.namespace}."${table}";`,
  ].join('\n')
}

function pyiceberg(data: Cluster, table: string): string {
  return [
    'from pyiceberg.catalog.rest import RestCatalog',
    '',
    `catalog = RestCatalog('mink', uri='${data.catalogUrl}', **{'s3.access-key-id': 'mink', 's3.secret-access-key': 'mink'})`,
    `table = catalog.load_table('${data.namespace}.${table}')`,
    'print(table.scan().to_arrow().num_rows)',
  ].join('\n')
}

function spark(data: Cluster, table: string): string {
  return [
    'spark-sql \\',
    '  --packages org.apache.iceberg:iceberg-spark-runtime-4.1_2.13:1.12.0,org.apache.iceberg:iceberg-aws-bundle:1.12.0 \\',
    '  --conf spark.sql.catalog.mink=org.apache.iceberg.spark.SparkCatalog \\',
    '  --conf spark.sql.catalog.mink.type=rest \\',
    `  --conf spark.sql.catalog.mink.uri=${data.catalogUrl} \\`,
    '  --conf spark.sql.catalog.mink.io-impl=org.apache.iceberg.aws.s3.S3FileIO \\',
    `  --conf spark.sql.catalog.mink.s3.endpoint=${data.s3Endpoint} \\`,
    '  --conf spark.sql.catalog.mink.s3.path-style-access=true \\',
    '  --conf spark.sql.catalog.mink.s3.access-key-id=mink \\',
    '  --conf spark.sql.catalog.mink.s3.secret-access-key=mink \\',
    `  -e 'SELECT count(*) FROM mink.${data.namespace}.\`${table}\`'`,
  ].join('\n')
}

function trino(data: Cluster): string {
  return [
    'connector.name=iceberg',
    'iceberg.catalog.type=rest',
    `iceberg.rest-catalog.uri=${data.catalogUrl}`,
    'fs.native-s3.enabled=true',
    `s3.endpoint=${data.s3Endpoint}`,
    's3.path-style-access=true',
    's3.region=us-east-1',
    's3.aws-access-key=mink',
    's3.aws-secret-key=mink',
  ].join('\n')
}
