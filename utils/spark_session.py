from databricks.connect import DatabricksSession
from databricks.sdk.core import Config


class SparkSession:
    def __init__(self):
        config = Config(
            cluster_id='0704-210611-1z4xoxac',

        )

        self.spark = DatabricksSession.builder.sdkConfig(config).getOrCreate()

    def get_spark(self):
        return self.spark
