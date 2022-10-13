# Databricks notebook source
lesson_name = "pipeline_demo"

# COMMAND ----------

# The DataFactory is just a pattern to demonstrate a fake stream is more of a function
# streaming workloads than it is of a pipeline - this pipeline happens to stream data.
class DataFactory:
    def __init__(self):
        
        # Bind the stream-source to DA because we will use it again later.
        DA.paths.stream_source = f"{DA.paths.working_dir}/stream-source"
        
        self.source_dir = f"{DA.paths.datasets}/retail-pipeline"
        self.target_dir = DA.paths.stream_source
        
        # All three datasets *should* have the same count, but just in case,
        # We are going to take the smaller count of the three datasets
        orders_count = len(dbutils.fs.ls(f"{self.source_dir}/orders/stream_json"))
        status_count = len(dbutils.fs.ls(f"{self.source_dir}/status/stream_json"))
        customer_count = len(dbutils.fs.ls(f"{self.source_dir}/customers/stream_json"))
        self.max_batch = min(min(orders_count, status_count), customer_count)
        
        self.current_batch = 0
        
    def load(self, continuous=False, delay_seconds=5):
        import time
        self.start = int(time.time())
        
        if self.current_batch >= self.max_batch:
            print("Data source exhausted\n")
            return False
        elif continuous:
            while self.load():
                time.sleep(delay_seconds)
            return False
        else:
            print(f"Loading batch {self.current_batch+1} of {self.max_batch}", end="...")
            self.copy_file("customers")
            self.copy_file("orders")
            self.copy_file("status")
            self.current_batch += 1
            print(f"{int(time.time())-self.start} seconds")
            return True
            
    def copy_file(self, dataset_name):
        source_file = f"{self.source_dir}/{dataset_name}/stream_json/{self.current_batch:02}.json/"
        target_file = f"{self.target_dir}/{dataset_name}/{self.current_batch:02}.json"
        dbutils.fs.cp(source_file, target_file)

# COMMAND ----------

class PipelineConfig():
    def __init__(self, pipeline_name, source, notebooks):
        self.pipeline_name = pipeline_name # The name of the pipeline
        self.source = source               # Custom Property
        self.notebooks = notebooks         # This list of notebooks for this pipeline
    
    def __repr__(self):
        content = f"Name:      {self.pipeline_name}\nSource:    {self.source}\n"""
        content += f"Notebooks: {self.notebooks.pop(0)}"
        for notebook in self.notebooks: content += f"\n           {notebook}"
        return content


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def get_pipeline_config(self, language):
    """
    Returns the configuration to be used by the student in configuring the pipeline.
    """
    base_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    base_path = "/".join(base_path.split("/")[:-1])
    
    pipeline_name = f"{DA.unique_name}"
    if DA.lesson_config.clean_name is not None: pipeline_name += f"-{DA.lesson_config.clean_name}"
    pipeline_name += ": Example Pipeline"
    
    if language is None: language = dbutils.widgets.getArgument("pipeline-language", None)
    assert language in ["SQL", "Python"], f"A valid language must be specified, found {language}"
    
    AB = "A" if language == "SQL" else "B"
    return PipelineConfig(pipeline_name, self.paths.stream_source, [
        f"{base_path}/DE 4.1{AB} - {language} Pipelines/DE 4.1.1 - Orders Pipeline",
        f"{base_path}/DE 4.1{AB} - {language} Pipelines/DE 4.1.2 - Customers Pipeline",
        f"{base_path}/DE 4.1{AB} - {language} Pipelines/DE 4.1.3 - Status Pipeline"
    ])


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def print_pipeline_config(self, language):
    """
    Renders the configuration of the pipeline as HTML
    """
    config = self.get_pipeline_config(language)
    
    width = "600px"
    
    html = f"""<table style="width:100%">
    <tr>
        <td style="white-space:nowrap; width:1em">Pipeline Name:</td>
        <td><input type="text" value="{config.pipeline_name}" style="width: {width}"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Source:</td>
        <td><input type="text" value="{config.source}" style="width: {width}"></td></tr>

        <td style="white-space:nowrap; width:1em">Target:</td>
        <td><input type="text" value="{self.schema_name}" style="width: {width}"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Storage Location:</td>
        <td><input type="text" value="{self.paths.storage_location}" style="width: {width}"></td></tr>
    """
    
    for i, path in enumerate(config.notebooks):
        html += f"""
        <tr>
            <td style="white-space:nowrap; width:1em">Notebook #{i+1} Path:</td>
            <td><input type="text" value="{path}" style="width: {width}"></td></tr>"""
    
    html += "</table>"
    
    displayHTML(html)


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def create_pipeline(self, language):
    """
    Creates the prescribed pipeline.
    """
    
    from dbacademy.dbrest import DBAcademyRestClient
    client = DBAcademyRestClient()

    config = self.get_pipeline_config(language)
    print(f"Creating the pipeline \"{config.pipeline_name}\"")

    # Delete the existing pipeline if it exists
    client.pipelines().delete_by_name(config.pipeline_name)

    # Create the new pipeline
    pipeline = client.pipelines().create(
        name = config.pipeline_name, 
        development=True,
        storage = self.paths.storage_location, 
        target = self.schema_name,
        notebooks = config.notebooks,
        configuration = {
            "source": config.source
        })
    
    self.pipeline_id = pipeline.get("pipeline_id")


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def start_pipeline(self):
    "Starts the pipeline and then blocks until it has completed, failed or was canceled"

    import time
    from dbacademy.dbrest import DBAcademyRestClient
    client = DBAcademyRestClient()

    # Start the pipeline
    start = client.pipelines().start_by_id(self.pipeline_id)
    update_id = start.get("update_id")

    # Get the status and block until it is done
    update = client.pipelines().get_update_by_id(self.pipeline_id, update_id)
    state = update.get("update").get("state")

    done = ["COMPLETED", "FAILED", "CANCELED"]
    while state not in done:
        duration = 15
        time.sleep(duration)
        print(f"Current state is {state}, sleeping {duration} seconds.")    
        update = client.pipelines().get_update_by_id(self.pipeline_id, update_id)
        state = update.get("update").get("state")
    
    print(f"The final state is {state}.")    
    assert state == "COMPLETED", f"Expected the state to be COMPLETED, found {state}"


# COMMAND ----------

@DBAcademyHelper.monkey_patch
def validate_pipeline_config(self, pipeline_language):
    "Provided by DBAcademy, this function validates the configuration of the pipeline"
    import json
    
    config = self.get_pipeline_config(pipeline_language)
    pipeline = self.client.pipelines().get_by_name(config.pipeline_name)
    
    suite = DA.tests.new("Pipeline Config")
    suite.test_not_none(pipeline, description=f"Create the pipeline \"<b>{config.pipeline_name}</b>\".", hint="Double check the spelling.")
    
    if pipeline is None: pipeline = {}
    spec = pipeline.get("spec", {})
    
    storage = spec.get("storage", None)
    suite.test_equals(storage, DA.paths.storage_location, f"Set the storage location to \"<b>{DA.paths.storage_location}</b>\".", hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\".")
    
    target = spec.get("target", None)
    suite.test_equals(target, DA.schema_name, f"Set the target to \"<b>{DA.schema_name}</b>\".", hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\".")
    
    libraries = spec.get("libraries", [])
    libraries = [l.get("notebook", {}).get("path") for l in libraries]
    
    def test_notebooks():
        if libraries is None: return False
        if len(libraries) != 3: return False
        for library in libraries:
            if library not in config.notebooks: return False
        return True
    
    hint = f"""Found the following {len(libraries)} notebook(s):<ul style="margin-top:0">"""
    for library in libraries:
        hint += f"""<li>{library}</li>"""
    hint += "</ul>"
    
    suite.test(test_function=test_notebooks, actual_value=libraries, description="Configure the three Notebook libraries", hint=hint)
    
    suite.test_length(spec.get("configuration", {}), 2, 
                      description=f"Set the two configuration parameters.", 
                      hint=f"Found [[LEN_ACTUAL_VALUE]] configuration parameter(s).")
    
    suite.test_equals(spec.get("configuration", {}).get("source"), config.source, 
                      description=f"Set the \"<b>source</b>\" configuration parameter to \"<b>{config.source}</b>\".", 
                      hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\".")
    
    suite.test_equals(spec.get("configuration", {}).get("spark.master"), "local[*]", 
                      description=f"Set the \"<b>spark.master</b>\" configuration parameter to \"<b>local[*]</b>\".", 
                      hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\".")
    
    suite.test_is_none(spec.get("clusters",[{}])[0].get("autoscale"), 
                       description=f"Autoscaling should be disabled.")
    
    suite.test_equals(spec.get("clusters", [{}])[0].get("num_workers"), 0, 
                      description=f"The number of spark workers should be 0.", 
                      hint=f"Found [[ACTUAL_VALUE]] workers.")

    suite.test_true(spec.get("development"), 
                    description=f"The pipeline mode should be set to \"<b>Development</b>\".")
    
    suite.test(test_function = lambda: {spec.get("channel") is None or spec.get("channel").upper() == "CURRENT"}, 
               actual_value=spec.get("channel"),
               description=f"The channel should be set to \"<b>Current</b>\".", 
               hint=f"Found \"<b>[[ACTUAL_VALUE]]</b>\"")
    
    suite.test_true(spec.get("photon"), 
                    description=f"Photon should be enabled.")
    
    suite.test_false(spec.get("continuous"), 
                     description=f"Expected the Pipeline mode to be \"<b>Triggered</b>\".", 
                     hint=f"Found \"<b>Continuous</b>\".")

    if suite.passed:
        policy = self.client.cluster_policies.get_by_name("Student's DLT-Only Policy")
        if policy is not None:
            cluster = { 
                "num_workers": 0,
                "label": "default", 
                "policy_id": policy.get("policy_id")
            }
            self.client.pipelines.create_or_update(name = config.pipeline_name,
                                                   storage = DA.paths.storage_location,
                                                   target = DA.schema_name,
                                                   notebooks = config.notebooks,
                                                   configuration = {
                                                       "spark.master": "local[*]",
                                                       "source": config.source,
                                                   },
                                                   clusters=[cluster])
    suite.display_results()


