# Installation & Execution Guide

## Prerequisites

To run this project, ensure you have the following installed on your host machine:

1.  **Docker Desktop**: Ensure it is running and configured (preferably using the WSL 2 backend on Windows).
2.  **Hosts Setup**: Since this is a local dev only environment, the following line needs to be added to the hosts file `127.0.0.1 datanode`
3.  **Visual Studio Code**: The recommended IDE.
4.  **Dev Containers Extension** for VS Code: To open the environment inside the container.

---

## Setup & Running with Dev Containers (Recommended)

This is the easiest way to get the environment running as it handles building and connecting to the docker-compose stack automatically.

1.  **Open the Project**:
    Open the root directory of this repository (`bdm`) in Visual Studio Code.

2.  **Reopen in Container**:
    *   When prompted by VS Code (bottom right notification), click **"Reopen in Container"**.
    *   Alternatively, open the Command Palette (`Ctrl+Shift+P`) and select **"Dev Containers: Reopen in Container"**.

3.  **Wait for Build**:
    VS Code will build the containers (if not already built) and start the services defined in `docker-compose.yml`. This might take a few minutes the first time.

4.  **Verify Connection**:
    Once the window reloads and the terminal is ready, you are now inside the `spark-client` container.
    You can verify the cluster is running by checking the Spark Master Web UI at [http://localhost:8080](http://localhost:8080) (forwarded from the container).

---

## Manual Execution (Docker Compose)

If you prefer not to use the Dev Containers extension, you can run the cluster manually:

1.  Open a terminal in the project root.
2.  Run the cluster:
    ```bash
    docker-compose up -d
    ```
3.  Access the client container:
    ```bash
    docker exec -it spark-client bash
    ```
4.  Navigate to the workspace:
    ```bash
    cd /workspace
    ```

---

## Running the Tasks

The project tasks are implemented as Jupyter Notebooks located in the `notebooks/` directory.

### Running Notebooks
1.  Inside the VS Code Dev Container, go to the `notebooks/` folder.
2.  Open `recommender_system.ipynb` (or any other notebook).
3.  Select the kernel: **Python 3**.
4.  Run the cells. The SparkSession is configured to connect to `spark://spark-master:7077`.

### Dependencies
The environment comes pre-installed with PySpark. Additional dependencies can be installed via `pip`:
```bash
# Inside the container
pip install -r requirements.txt
```
(Note: `pandas` and `pyarrow` are auto-installed by the `postCreateCommand` in `devcontainer.json`).
