from optparse import make_option

from django.conf import settings
from django.core.management.base import NoArgsCommand
from django.db import connections, router, models, DEFAULT_DB_ALIAS
from django.utils.datastructures import SortedDict
from django.utils.importlib import import_module
import re

MAP = 'map'
TYPES_REQUEST = 'types_request'

MYSQL_SETTINGS = {
    MAP: {
        'integer': 'int',
        'bool': 'tinyint(1)',
        'date': 'date',
        'datetime': 'datetime',
        'longtext': 'longtext',
        'smallint': 'smallint',
        'double precision': 'double',
    },
    TYPES_REQUEST: "select column_name, column_type from INFORMATION_SCHEMA.COLUMNS where table_name = '%s'",
}

POSTGRESQL_SETTINGS = {
    MAP: {
        'varchar': 'character',
        'integer': 'integer',
        'serial': 'integer',
        'boolean': 'boolean',
        'date': 'date',
        'timestamp with time zone': 'timestamp with time zone',
        'text': 'text',
        'smallint': 'smallint',
        'double precision': 'double precision',
        'numeric': 'numeric',
    },
    TYPES_REQUEST: "select column_name, data_type from INFORMATION_SCHEMA.COLUMNS where table_name = '%s'",
}

varchar_exp = re.compile(r'varchar\((\d+)\)')
VARCHAR = 'varchar'

def colorize(string, bold=True, green=False):
    prefix = '1;' if bold else ''
    color = '32' if green else '31'
    return '\x1b[%s%sm%s\x1b[0m' % (prefix, color, string)
    
def pretty_name(model):
    return '%s.%s' % (model._meta.app_label, model._meta.object_name)

def pretty_list(lst):
    return ', '.join([item for item in sorted(lst)])

class Command(NoArgsCommand):
    option_list = NoArgsCommand.option_list + (
        make_option('--database', action='store', dest='database',
            default=DEFAULT_DB_ALIAS, help='Nominates a database to check. '
                'Defaults to the "default" database.'),
    )
    help = "Check correspondence between the models and database tables for all apps in INSTALLED_APPS."
    
    def handle_noargs(self, **options):
        db = options.get('database')
        connection = connections[db]
        cursor = connection.cursor()
        is_postgres = 'postgres' in settings.DATABASES[db]['ENGINE']
        DB_SETTINGS = POSTGRESQL_SETTINGS if is_postgres else MYSQL_SETTINGS

        # Get a list of already installed *models* so that references work right.
        tables = connection.introspection.table_names()

        # Build the manifest of apps and models that are to be synchronized
        all_models = [
            (app.__name__.split('.')[-2],
                [m for m in models.get_models(app, include_auto_created=True)
                if router.allow_syncdb(db, m)])
            for app in models.get_apps()
        ]

        def model_installed(model):
            opts = model._meta
            converter = connection.introspection.table_name_converter
            return not ((converter(opts.db_table) in tables) or
                (opts.auto_created and converter(opts.auto_created._meta.db_table) in tables))

        manifest = SortedDict(
            (app_name, list(filter(model_installed, model_list)))
            for app_name, model_list in all_models
        )
        
        new_models = []
        for each in manifest.values():
            new_models += each
        if len(new_models) > 0:
            print colorize('[ERROR] Migration is needed')
            print 'Unregistered models:'
            for each in manifest.items():
                for model in each[1]:
                    print ' %s' % pretty_name(model)
            exit(1)

        table_info = []
        tables = connection.introspection.table_names()
        seen_models = connection.introspection.installed_models(tables)
        for model in seen_models:
            table = model._meta.db_table
            columns = [field.column for field in model._meta.fields]
            types = {}
            for field in model._meta.fields:
                types[field.column] = field.db_type(connection=connection)
                
            # issue with inheritance
            parents = model._meta.parents
            for parent in parents:
                for field in parent._meta.fields:
                    columns.remove(field.column)
                    del types[field.column]

            table_info.append((table, columns, types))
            
        for model in seen_models:
            for field in model._meta.local_many_to_many:
                if hasattr(field, 'creates_table') and not field.creates_table:
                    continue
                table = field.m2m_db_table()
                columns = ['id'] # They always have an id column
                types['id'] = 'integer'
                columns.append(field.m2m_column_name())
                columns.append(field.m2m_reverse_name())
                types[field.m2m_column_name()] = 'integer'
                types[field.m2m_reverse_name()] = 'integer'
                table_info.append((table, columns, types))
                
        
        out_of_sync = []
        for app_name, model_list in all_models:
            for model in model_list:
                opts = model._meta
                converter = connection.introspection.table_name_converter
                table_name = converter(opts.db_table)
                for item in table_info:
                    if item[0] == table_name:
                        cursor.execute(DB_SETTINGS[TYPES_REQUEST] % table_name)
                        content = cursor.fetchall()
                        actual_columns = [column[0] for column in content]
                        if set(item[1]) != set(actual_columns):
                            print colorize('[ERROR] Model fields are out of sync: %s' % pretty_name(model), bold=False)
                            print ' Model fields:       %s' % pretty_list(item[1])
                            print ' Database columns:   %s' % pretty_list(actual_columns)
                            out_of_sync += [model]
                            break
                        for djcolname, djcoltype in item[2].items():
                            dbtypebase = None
                            for coltype in DB_SETTINGS[MAP]:
                                if coltype in djcoltype:
                                    dbtypebase = DB_SETTINGS[MAP][coltype]
                                    break
                            if not dbtypebase:
                                m = varchar_exp.search(djcoltype)
                                if m:
                                    dbtypebase = VARCHAR
                                    varchar_length = int(m.group(1))
                            if not dbtypebase:
                                print colorize('[ERROR] Can\'t validate DB. Unknown field type in %s.%s: %s' % (pretty_name(model), djcolname, djcoltype))
                                exit(1)
                            for dbcolname, dbcoltype in content:
                                if dbcolname == djcolname:
                                    if dbtypebase not in dbcoltype:
                                        print colorize('[ERROR] Inconsistent field type in model \'%s\'' % pretty_name(model), bold=False)
                                        print ' Model field:       %s %s' % (djcolname, djcoltype)
                                        print ' Database column:   %s %s' % (dbcolname, dbcoltype)
                                        out_of_sync += [model]
                                        break
                                    if dbtypebase == VARCHAR:
                                        m = varchar_exp.search(dbcoltype)
                                        if not m or varchar_length != int(m.group(1)):
                                            print colorize('[ERROR] Inconsistent varchar length in model \'%s\'' % pretty_name(model), bold=False)
                                            print ' Model field:       %s %d' % (djcolname, varchar_length)
                                            print ' Database column:   %s %d' % (dbcolname, int(m.group(1)))
                                            out_of_sync += [model]
                                        break
                        break
                else:
                    print colorize('[ERROR] Can\'t validate DB. There\'s must be an error in the script!')
                    exit(1)
        if out_of_sync:
            print colorize('[ERROR] Migration is needed')
            print ' Model fields are out of sync:'
            for each in set(out_of_sync):
                print ' %s' % pretty_name(each)
            exit(1)
            
        print colorize('[OK] Migration checks passed', bold=False, green=True)
        