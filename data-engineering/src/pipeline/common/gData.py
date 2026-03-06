"""
@description: Path manager for config files.
@author: Rithwik Babu
"""
import os

base_dir = os.path.dirname(__file__)
config_dir = os.path.join(base_dir, "..", "..", "..", "config")

# gcp
gcp_directory = os.path.join(config_dir, "gcp")
production_gcp_config = os.path.join(gcp_directory, "production.yaml")
development_gcp_config = os.path.join(gcp_directory, "development.yaml")

# tables
table_directory = os.path.join(config_dir, "tables")
fields_table = os.path.join(table_directory, "fields.json")
hawk_identifiers_table = os.path.join(table_directory, "hawk_identifiers.json")
