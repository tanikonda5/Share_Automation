import pdb

import snowflake.connector
import hvac
from cryptography.hazmat.backends import default_backend
from cryptography.hazmat.primitives import serialization
from ConfigObject import ConfigObject
import argparse
import sys


def open_database_connection(config):
    # Connect to Keeper to collect secrets
    client = hvac.Client(
        url=config.config_properties.keeper_uri,
        namespace=config.config_properties.keeper_namespace,
        token=config.config_properties.keeper_token
    )
    # Secrets are stored within the key entitled 'data'
    keeper_secrets = client.read(config.config_properties.secret_path)['data']
    passphrase = keeper_secrets['SNOWSQL_PRIVATE_KEY_PASSPHRASE']
    private_key = keeper_secrets['private_key']

    # PEM key must be byte encoded
    key = bytes(private_key, 'utf-8')

    p_key = serialization.load_pem_private_key(
        key
        , password=passphrase.encode()
        , backend=default_backend()
    )

    pkb = p_key.private_bytes(
        encoding=serialization.Encoding.DER
        , format=serialization.PrivateFormat.PKCS8
        , encryption_algorithm=serialization.NoEncryption())

    conn = snowflake.connector.connect(
        user=config.config_properties.user
        , account=config.config_properties.account
        , warehouse=config.config_properties.warehouse
        , role=config.config_properties.role
        , database=config.config_properties.database
        , schema=config.config_properties.schema
        , timezone=config.config_properties.timezone
        # , password=config.config_properties.password
        , private_key=pkb
    )
    return conn


def check_arg(args=None):
    parser = argparse.ArgumentParser(description='Script to learn basic argparse')
    parser.add_argument('-d', '--p_database',
                        help='provider_database',
                        required=True)
    parser.add_argument('-c', '--c_database',
                        help='consumer_database',
                        required=True)
    parser.add_argument('-s', '--schemas',
                        help='schemas',
                        default=None)
    parser.add_argument('-t', '--tables',
                        help='tables',
                        default=None)
    parser.add_argument('-p', '--provider_conf',
                        help='--provider_conf',
                        default='provider_config.ini')
    parser.add_argument('-cf', '--customer_conf',
                        help='--customer_conf',
                        default='consumer_config.ini')

    results = parser.parse_args(args)
    return results.p_database, results.c_database, results.schemas, results.tables, results.provider_conf, results.customer_conf


def display_share(cs, share):
    print("Grants to share %s" % share)
    sql = "show grants to share %s" % share
    cs.execute(sql)
    for grant in cs.fetchall():
        print(grant)

    print("%s share:" % share)
    cs.execute("show shares like '%s'" % share)
    for s in cs.fetchall():
        print(s)


def create_share(cs, database, schemas, tables, account, share):
    #
    if schemas is not None:
        schema_list = set(schemas.split(","))

    if tables is not None:
        table_list = set(tables.split(","))

    sql = "use %s" % database
    cs.execute(sql)
    cs.execute("use role accountadmin")
    sql = "create or replace share %s" % share
    cs.execute(sql)
    sql = "grant usage on database %s to share %s" % (database, share)
    cs.execute(sql)
    cs.execute("show schemas")
    for schema in cs.fetchall():
        if schemas is not None:
            if not schema[1] in schema_list:
                continue
        try:
            sql = "grant usage on schema %s.%s to share %s" % (database, schema[1], share)
            cs.execute(sql)
            sql = "use schema %s" % (schema[1])
            cs.execute(sql)
        except:
            continue

        cs.execute("show tables")
        for table in cs.fetchall():
            if tables is not None:
                if not table[1] in table_list:
                    continue

            sql = "grant select on %s.%s.%s to share %s" % (database, schema[1], table[1], share)
            cs.execute(sql)

    sql = "alter share %s add accounts=%s" % (share, account)
    cs.execute(sql)


def display_database_from_share(cs, database, warehouse):
    sql = "use warehouse %s" % warehouse
    cs.execute(sql)
    sql = "use %s" % database
    cs.execute(sql)
    sql = "select current_warehouse(), current_database(), current_schema()"
    cs.execute(sql)
    print(cs.fetchone())

    cs.execute("show schemas")
    for schema in cs.fetchall():
        print(schema[1])
        sql = "use schema %s" % (schema[1])
        cs.execute(sql)
        cs.execute("show tables")
        for table in cs.fetchall():
            print("    %s" % (table[1]))


def transfer_data_from_share(cs, warehouse, database, share):
    sql = "use warehouse %s" % warehouse
    cs.execute(sql)
    sql = "create database if not exists %s" % database
    cs.execute(sql)
    sql = "use %s" % share
    cs.execute(sql)

    cs.execute("show schemas")
    schema_list = cs.fetchall()
    sql = "use database %s" % database
    cs.execute(sql)
    for schema in schema_list:
        try:
            sql = "create schema if not exists %s" % (schema[1])
            cs.execute(sql)
        except:
            continue

    sql = "use database %s" % share
    cs.execute(sql)
    for schema in schema_list:
        # print(schema[1])
        sql = "use schema %s" % (schema[1])
        cs.execute(sql)
        cs.execute("show tables")
        for table in cs.fetchall():
            # print("    %s" % (table[1]))
            # cs.execute("use  database %s" % database)
            # cs.execute("use schema %s" % (schema[1]))
            # cs.execute("show grants on %s.%s.%s" % (database, schema[1], table[1]))
            # grants = cs.fetchall()
            # grant_set = grants[1]
            # grant_list = list(grant_set)
            # role = grant_list[5]
            # cs.execute("use role edw_datalake_role")
            sql = """
            create table %s.%s.%s as select * from %s.%s.%s
            """ % (database, schema[1], table[1], share, schema[1], table[1])
            cs.execute(sql)
            cs.execute("use database %s" % database)
            cs.execute("use schema %s" % (schema[1]))
            cs.execute("grant ownership on %s to edw_datalake_role copy current grants;" % table[1])
            cs.execute("grant select on %s  to role EDW_TELMTRY_CDPTELE_ETL_ROLE" % table[1])
            cs.execute("grant select on %s  to role EDW_SERVICE_CDP_ETL_ROLE" % table[1])


def create_database_from_share(cs, database, account, share):
    cs.execute("use role accountadmin")
    sql = "create or replace database %s from share %s.%s" % (database, account, share)
    cs.execute(sql)
    sql = "grant imported privileges on database %s to role sysadmin" % database
    cs.execute(sql)


def drop_share(cs, share):
    sql = "drop share %s" % share
    cs.execute(sql)


def dropDatabaseFromShare(cs, database):
    sql = "drop database %s" % database
    cs.execute(sql)


def main():
    p_database, c_database, schemas, tables, provider_conf, customer_conf = check_arg(sys.argv[1:])
    if schemas is None and tables is not None:
        print("Schema is required for table")
        sys.exit(1)

    # provider_conf = "provider_config.ini"
    # customer_conf = "consumer_config.ini"
    # database = "EDW_SERVICES_DB_BACKUP_DV3"
    # schemas = "PUBLIC,SERVICES_BR,SERVICES_INCR"
    # schemas = None
    # tables = "DUMMY_EMP,TEST"
    # tables = "CE_USAGE_FINAL"
    # tables = None
    # customer_account = "CISCOSTAGE"
    # provider_account = "CISCODEV"

    share = "%s_SHARE" % c_database
    config_provider = ConfigObject(filename=provider_conf)
    ctx_provider = open_database_connection(config_provider)
    cs_provider = ctx_provider.cursor()

    config_customer = ConfigObject(filename=customer_conf)
    ctx_customer = open_database_connection(config_customer)
    cs_customer = ctx_customer.cursor()

    warehouse = str(config_customer.config_properties.warehouse)
    customer_account = str(config_customer.config_properties.account).split(".")[0]
    provider_account = str(config_provider.config_properties.account).split(".")[0]

    try:
        create_share(cs_provider, p_database, schemas, tables, customer_account, share)
        display_share(cs_provider, share)
        create_database_from_share(cs_customer, share, provider_account, share)
        display_database_from_share(cs_customer, share, warehouse)
        transfer_data_from_share(cs_customer, warehouse, c_database, share)
        display_database_from_share(cs_customer, c_database, warehouse)
        drop_share(cs_provider, share)
        dropDatabaseFromShare(cs_customer, share)
        display_database_from_share(cs_customer, c_database, warehouse)
        # dropDatabaseFromShare(cs_customer, database)
    finally:
        cs_provider.close()
        cs_customer.close()
    ctx_provider.close()
    ctx_customer.close()


if __name__ == '__main__':
    main()
