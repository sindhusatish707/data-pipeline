from airflow.plugins_manager import AirflowPlugin
from slack_utils import slack_notify

class SlackPlugin(AirflowPlugin):
    name = "slack_plugin"
    hooks = []
    operators = []
    sensors = []
    macros = []
    appbuilder_views = []
    appbuilder_menu_items = []
    flask_blueprints = []
