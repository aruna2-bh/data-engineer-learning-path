# Databricks notebook source
# Normally this logic would be in a specific classroom-setup file if used by only one lesson or in _utility-funtions if used by multiple.
# In this case, it is refactored into a sepearte notebook to make the concepts/patterns easier to understand.

# COMMAND ----------

# The DataFactory is just a pattern to demonstrate a fake stream is more of a function
# streaming workloads than it is of a pipeline - this pipeline happens to stream data.
class DataFactory:
    def __init__(self):
        
        # Bind the stream-source to DA because we will use it again later.
        DA.paths.stream_source = f"{DA.paths.working_dir}/stream-source"
        
        self.source_dir = f"{DA.paths.datasets}/sales"
        self.target_dir = DA.paths.stream_source
        
        # All three datasets *should* have the same count, but just in case,
        # We are going to take the smaller count of the three datasets
        orders_count = len(dbutils.fs.ls(f"{self.source_dir}/orders"))
        status_count = len(dbutils.fs.ls(f"{self.source_dir}/status"))
        customer_count = len(dbutils.fs.ls(f"{self.source_dir}/customers"))
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
        file = f"{dataset_name}/{self.current_batch:02}.json"
        source_file = f"{self.source_dir}/{file}"
        target_file = f"{self.target_dir}/{file}"
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

def get_pipeline_config(self, from_job=False):
    """
    Returns the configuration to be used by the student in configuring the pipeline.
    """
    base_path = dbutils.entry_point.getDbutils().notebook().getContext().notebookPath().getOrElse(None)
    base_path = "/".join(base_path.split("/")[:-1])
    
    da_name, da_hash = DA.get_username_hash()
    pipeline_name = f"da-{da_name}-{da_hash}-{self.course_code.lower()}"
    if DA.clean_lesson is not None: pipeline_name += f"-{DA.clean_lesson}"
    pipeline_name += ": Example Pipeline"
    
    try:
        # From a job or the pipline was pre-created
        if from_job or DA.pipeline_id: pipeline_name += " from Job"
    except: pass # ignore any errors
    
    return PipelineConfig(pipeline_name, self.paths.stream_source, [
        f"{base_path}/EC 10.B - Pipelines/EC 10.B.1 - Orders Pipeline",
        f"{base_path}/EC 10.B - Pipelines/EC 10.B.2 - Customers Pipeline",
        f"{base_path}/EC 10.B - Pipelines/EC 10.B.3 - Status Pipeline",
    ])

DBAcademyHelper.monkey_patch(get_pipeline_config)

# COMMAND ----------

def print_pipeline_config(self):
    """
    Renders the configuration of the pipeline as HTML
    """
    config = self.get_pipeline_config()
    
    width = "600px"
    
    html = f"""<table style="width:100%">
    <tr>
        <td style="white-space:nowrap; width:1em">Pipeline Name:</td>
        <td><input type="text" value="{config.pipeline_name}" style="width: {width}"></td></tr>
    <tr>
        <td style="white-space:nowrap; width:1em">Source:</td>
        <td><input type="text" value="{config.source}" style="width: {width}"></td></tr>

        <td style="white-space:nowrap; width:1em">Target:</td>
        <td><input type="text" value="{self.db_name}" style="width: {width}"></td></tr>
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

DBAcademyHelper.monkey_patch(print_pipeline_config)

# COMMAND ----------

def create_pipeline(self, from_job=False):
    """
    Creates the prescribed pipline.
    """
    
    from dbacademy.dbrest import DBAcademyRestClient
    client = DBAcademyRestClient()

    config = self.get_pipeline_config(from_job)
    print(f"Creating the pipeline \"{config.pipeline_name}\"")

    # Delete the existing pipeline if it exists
    client.pipelines().delete_by_name(config.pipeline_name)

    # Create the new pipeline
    pipeline = client.pipelines().create(
        name = config.pipeline_name, 
        development=True,
        storage = self.paths.storage_location, 
        target = self.db_name, 
        notebooks = config.notebooks,
        configuration = {
            "source": config.source,
            "pipelines.applyChangesPreviewEnabled": True
        })
    
    self.pipeline_id = pipeline.get("pipeline_id")
       
DBAcademyHelper.monkey_patch(create_pipeline)

# COMMAND ----------

def start_pipeline(self):
    "Starts the pipline and then blocks until it has completed, failed or was canceled"

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

DBAcademyHelper.monkey_patch(start_pipeline)

