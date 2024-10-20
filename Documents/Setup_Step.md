# Set Up Environment For Project

## I. Download and Set Up Ubuntu

<details><summary><b>I.1 Using Terminal with `wsl --list --online`</b></summary>

**1. List available distributions**:
   - Open a terminal and enter the command:
     ```
     wsl --list --online
     ```
   - This command will display a list of available Linux distributions.

**2. Install Ubuntu**:
   - To install Ubuntu, enter:
     ```
     wsl --install -d Ubuntu
     ```
   - Wait for the installation to complete.

**3. Set up username and password**:
   - Once installed, open Ubuntu through WSL and set up a username and password for the initial login.
</details>

<details><summary><b> I.2 Using Tar File </b></summary>

**1. Download WSL Ubuntu TAR File**:
   - Go to [WSL GitHub Releases](https://github.com/microsoft/WSL/releases) and download the latest `.tar.gz` file for Ubuntu.

**2. Install Ubuntu from TAR File**:
   - Use the following command to install the TAR file:
     ```
     wsl --import <DistributionName> <InstallLocation> <TARFilePath>
     ```
   - Replace `<DistributionName>` with your chosen name, `<InstallLocation>` with the directory path where you want to install Ubuntu, and `<TARFilePath>` with the path to the downloaded TAR file.
   - Example:
     ```
     wsl --import Ubuntu-22.04 C:\wsl\Ubuntu-22.04 C:\Downloads\ubuntu-22.04.tar.gz
     ```

**3. Set up username and password**:
   - Open the installed Ubuntu distribution and set up your username and password for login.
</details>

## II. Install Required Packages and Tools For Ubuntu

<details><summary><b> II.1 Install & Set Up Java JDK </b></summary>

### Java

<details><summary><b> 1. Install Java Using Terminal </b></summary>

**1. Open WSL with Terminal**:  
   - Launch WSL from your terminal or use the Ubuntu application.

**2. Install Java Using Terminal**:
   - Use the following command to install Java (e.g., JDK 17):
     ```bash
     sudo apt install openjdk-17-jdk openjdk-17-jre -y
     ```

**3. Set up Java Path in WSL Environment**:
   - Add the following lines to your `~/.bashrc` file to set the Java path:
     ```bash
     export JAVA_HOME=$(update-alternatives --query java | grep 'Value: ' | awk '{print $2}' | sed 's:/bin/java::')
     export PATH=$PATH:$JAVA_HOME/bin
     ```
   - Apply the changes:
     ```bash
     source ~/.bashrc
     ```

**4. Check Java Version in WSL Environment**:
   - Verify the Java installation with:
     ```bash
     java -version
     ```

</details>

<details><summary><b> 2. Install Java Using Java JDK Tar file </b></summary>

**1. Download Java**:
   - Go to [Java Tar](https://www.oracle.com/java/technologies/javase-jdk17-downloads.html) and download the latest `.tar.gz` file for Java (e.g., JDK 17).

**2. Process Java TAR File**:
   - Extract the tar file:
     ```bash
     tar -xvf <jdk_tar_file_name>.tar.gz -C /opt
     ```
   - Replace `<jdk_tar_file_name>` with the actual filename.

**3. Set up Java Path in WSL Environment**:
   - Add the following lines to your `~/.bashrc` file to set the Java path:
     ```bash
     export JAVA_HOME=/opt/<jdk_folder_name>
     export PATH=$PATH:$JAVA_HOME/bin
     ```
   - Replace `<jdk_folder_name>` with the extracted JDK folder name.
   - Apply the changes:
     ```bash
     source ~/.bashrc
     ```

**4. Check Java Version in WSL Environment**:
   - Verify the Java installation with:
     ```bash
     java -version
     ```

</details>

---

</details>

<details><summary><b> II.2 Install & Set Up Python</b></summary>

### Python 3.12

**1. Open WSL with Terminal**:  
   - Launch WSL from your terminal or use the Ubuntu application.

**2. Install Python 3.12 Using Terminal**:
   - Use the following command to install Python 3.12:
     ```bash
     sudo apt install python3.12
     ```

**3. Install pip for Python 3.12**:
   - Use the following command to install pip:
     ```bash
     sudo apt install python3-pip -y
     ```

**4. Install Python 3.12 Virtual Environment**:
   - Use the following command to install the Python 3.12 virtual environment module:
     ```bash
     sudo apt install python3.12-venv -y
     ```

**5. Check Python and pip Version in WSL Environment**:
   - Verify the Python and pip installation with:
     ```bash
     python3.12 --version && pip3.12 --version
     ```

---

</details>

<details><summary><b> II.3 Install & Set Up Scala 2.13.6 </b></summary>

### Scala 2.13.6

<details><summary><b> Scala.2.13.6 Install Using Terminal </b></summary>

**1. Open WSL with Terminal**:  
   - Launch WSL from your terminal or use the Ubuntu application.

**2. Update and Upgrade System**:
   - Update the package list and upgrade packages:
     ```bash
     sudo apt-get update
     sudo apt-get upgrade
     ```

**3. Download Scala 2.13.6**:
   - Use `wget` to download Scala:
     ```bash
     wget https://downloads.lightbend.com/scala/2.13.6/scala-2.13.6.tgz
     ```

**4. Extract the Scala TAR File**:
   - Extract the downloaded file:
     ```bash
     tar -xvzf scala-2.13.6.tgz
     ```

**5. Move Scala to /usr/bin**:
   - Move the extracted folder to `/usr/bin`:
     ```bash
     sudo mv scala-2.13.6 /usr/bin/scala
     ```

**6. Set up Scala Path in WSL Environment**:
   - Add the following lines to your `~/.bashrc` file:
     ```bash
     export SCALA_HOME=/usr/bin/scala
     export PATH=$SCALA_HOME/bin:$PATH
     ```
   - Apply the changes:
     ```bash
     source ~/.bashrc
     ```

**7. Remove the Downloaded File**:
   - Navigate to the home directory and delete the `.tgz` file:
     ```bash
     cd ~
     rm scala-2.13.6.tgz
     ```

</details>

<details><summary><b> Scala.2.13.6 Install Using Package Manager </b></summary>

**1. Open WSL with Terminal**:  
   - Launch WSL from your terminal or use the Ubuntu application.

**2. Update and Upgrade System**:
   - Update the package list and upgrade packages:
     ```bash
     sudo apt-get update
     sudo apt-get upgrade
     ```

**3. Install Scala from Package Manager**:
   - Use the following command to install Scala:
     ```bash
     sudo apt-get install scala -y
     ```

**4. Check Scala Version in WSL Environment**:
   - Verify the Scala installation with:
     ```bash
     scala -version
     ```

</details>

---

</details>

<details><summary><b> II.4 Install & Set Up Maven 3.9.9 </b></summary>

### Maven 3.9.9

<details><summary><b> 1. Install Using Terminal </b></summary>

**1. Open WSL with Terminal**:  
   - Launch WSL from your terminal or use the Ubuntu application.

**2. Download Maven 3.9.9**:
   - Use `wget` to download Maven:
     ```bash
     wget https://dlcdn.apache.org/maven/maven-3/3.9.9/binaries/apache-maven-3.9.9-bin.tar.gz
     ```

**3. Extract the Maven TAR File**:
   - Extract the downloaded file:
     ```bash
     tar -xvzf apache-maven-3.9.9-bin.tar.gz
     ```

**4. Move Maven to /opt**:
   - Move the extracted folder to `/opt`:
     ```bash
     sudo mv apache-maven-3.9.9 /opt/maven
     ```

**5. Set up Maven Path in WSL Environment**:
   - Add the following lines to your `~/.bashrc` file:
     ```bash
     export M2_HOME=/opt/maven
     export PATH=$M2_HOME/bin:$PATH
     ```
   - Apply the changes:
     ```bash
     source ~/.bashrc
     ```

**6. Remove the Downloaded File**:
   - Delete the `.tar.gz` file:
     ```bash
     rm apache-maven-3.9.9-bin.tar.gz
     ```

**7. Check Maven Version in WSL Environment**:
   - Verify the Maven installation with:
     ```bash
     mvn -version
     ```

</details>

<details><summary><b> 2. Install Using Package Manager </b></summary>

**1. Open WSL with Terminal**:  
   - Launch WSL from your terminal or use the Ubuntu application.

**2. Update and Upgrade System**:
   - Update the package list and upgrade packages:
     ```bash
     sudo apt-get update
     sudo apt-get upgrade
     ```

**3. Install Maven from Package Manager**:
   - Use the following command to install Maven:
     ```bash
     sudo apt-get install maven -y
     ```

**4. Check Maven Version in WSL Environment**:
   - Verify the Maven installation with:
     ```bash
     mvn -version
     ```

#### Option: Specific for the version (<b>Replace Step 3 by Step 5</b>)



**5. Add Maven 3.9.9 Repository**:
   - Add the official repository for Maven 3.9.9:
     ```bash
     sudo add-apt-repository ppa:andrei-pozolotin/maven3
     sudo apt-get update
     ```

**6. Install Maven 3.9.9 from Package Manager**:
   - Use the following command to install Maven 3.9.9:
     ```bash
     sudo apt-get install maven=3.9.9-1~ppa1~ubuntu -y
     ```

**7. Check Maven Version in WSL Environment**:
   - Verify the Maven installation with:
     ```bash
     mvn -version
     ```

</details>

---

</details>

<details><summary><b> II.5 Install & Set Up Apache Spark 3.5.3 </b></summary>

### Spark 3.5.3

**Prerequisites**: Ensure you have already installed [Java](#java), [Scala](#scala-2136), [Maven](#maven-399), and [Python](#python).

<details><summary><b> 1. Install Using Terminal </b></summary>

**1. Open WSL with Terminal**:  
   - Launch WSL from your terminal or use the Ubuntu application.

**2. Download Spark 3.5.3**:
   - Use `wget` to download Spark:
     ```bash
     wget https://archive.apache.org/dist/spark/spark-3.5.3/spark-3.5.3.tgz
     ```

**3. Create Spark Directory**:
   - Create a directory for Spark and move the downloaded file:
     ```bash
     mkdir ~/spark
     mv spark-3.5.3.tgz ~/spark/
     cd ~/spark
     ```

**4. Extract the Spark TAR File**:
   - Extract the downloaded file:
     ```bash
     tar -xvzf spark-3.5.3.tgz
     ```

**5. Remove the Downloaded File**:
   - Delete the `.tgz` file:
     ```bash
     rm spark-3.5.3.tgz
     ```

**6. Set up Spark Path in WSL Environment**:
   - Add the following lines to your `~/.bashrc` file:
     ```bash
     export SPARK_HOME=$(ls -d ~/spark/spark-* | sort -V | tail -n 1)
     export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH
     ```
   - Apply the changes:
     ```bash
     source ~/.bashrc
     ```

**7. Verify Spark Path**:
   - Check if Spark's home path is set correctly:
     ```bash
     echo $SPARK_HOME
     ```

**8. Build Spark Using Maven**:
   - Navigate to the Spark directory:
     ```bash
     cd ~/spark/spark-3.5.3
     ```
   - Build Spark using Maven, skipping tests:
     ```bash
     ./build/mvn -DskipTests clean package
     ```
   - Return to the home directory:
     ```bash
     cd ~
     ```

**9. Check Spark Version in WSL Environment**:
   - Run the following command to check if Spark is installed correctly:
     ```bash
     spark-shell --version
     ```

</details>

---

</details>

<details><summary><b> II.6 Install & Set Up Apache Kafka 3.8.0 </b></summary>

### Apache Kafka 3.8.0

<details><summary><b> 1. Install Using Terminal </b></summary>

**1. Open WSL with Terminal**:  
   - Launch WSL from your terminal or use the Ubuntu application.

**2. Update and Upgrade System**:
   - Update the package list and upgrade packages:
     ```bash
     sudo apt update
     sudo apt upgrade -y
     ```

**3. Download Kafka 3.8.0**:
   - Use `wget` to download Kafka:
     ```bash
     wget https://downloads.apache.org/kafka/3.8.0/kafka_2.13-3.8.0.tgz
     ```

**4. Extract the Kafka TAR File**:
   - Extract the downloaded file:
     ```bash
     tar -xzf kafka_2.13-3.8.0.tgz
     ```

**5. Move Kafka to /usr/bin**:
   - Move the extracted folder to `/usr/bin`:
     ```bash
     sudo mv kafka_2.13-3.8.0 /usr/bin/kafka
     ```

**6. Set up Kafka Path in WSL Environment**:
   - Add the following lines to your `~/.bashrc` file:
     ```bash
     export KAFKA_HOME=/usr/bin/kafka
     export PATH=$PATH:$KAFKA_HOME/bin
     ```
   - Apply the changes:
     ```bash
     source ~/.bashrc
     ```

**7. Configure Kafka**:

- **Edit the Kafka Server Properties File**:
  - Open the configuration file:
    ```bash
    sudo nano /usr/bin/kafka/config/server.properties
    ```
  - Find the following lines:
    ```
    log.dirs=/tmp/kafka-logs
    zookeeper.connect=localhost:2181
    listeners=PLAINTEXT://:9092
    ```
  - Change them to:
    ```
    log.dirs=/usr/bin/kafka/kafka-logs
    zookeeper.connect=localhost:2182
    listeners=PLAINTEXT://:9095
    ```
  - Save the changes with `CTRL + S` and exit with `CTRL + X`.

- **Edit the ZooKeeper Properties File**:
  - Open the configuration file:
    ```bash
    sudo nano /usr/bin/kafka/config/zookeeper.properties
    ```
  - Find the following lines:
    ```
    dataDir=/tmp/zookeeper
    clientPort=2181
    ```
  - Change them to:
    ```
    dataDir=/usr/bin/kafka/zookeeper
    clientPort=2182
    ```
  - Save the changes with `CTRL + S` and exit with `CTRL + X`.

**8. Start ZooKeeper**:
   - Navigate to the Kafka bin directory and start ZooKeeper:
     ```bash
     cd /usr/bin/kafka/bin
     sudo ./zookeeper-server-start.sh ../config/zookeeper.properties
     ```

**9. Start Kafka Broker**:
   - Open a new terminal window and run:
     ```bash
     wsl -d Ubuntu-24.04
     cd /usr/bin/kafka/bin
     sudo ./kafka-server-start.sh ../config/server.properties
     ```

**10. Test Kafka Installation**:

- **Create a Topic**:
  - Open a new terminal window and run:
    ```bash
    wsl -d Ubuntu-24.04
    cd /usr/bin/kafka/bin
    sudo ./kafka-topics.sh --create --topic test --bootstrap-server localhost:9095 --partitions 1 --replication-factor 1
    
    ```

- **List Topics**:
  - Run:
    ```bash
    sudo ./kafka-topics.sh --list --bootstrap-server localhost:9095
    ```

- **Send Messages to the Topic**:
  - Open a new terminal window and run:
    ```bash
    wsl -d Ubuntu-24.04
    cd /usr/bin/kafka/bin
    sudo ./kafka-console-producer.sh --topic test --bootstrap-server localhost:9095
    ```

- **Consume Messages from the Topic**:
  - Open a new terminal window and run:
    ```bash
    wsl -d Ubuntu-24.04
    cd /usr/bin/kafka/bin
    ./kafka-console-consumer.sh --topic test --from-beginning --bootstrap-server localhost:9095
    ```

</details>

---

</details>

<details><summary><b> II.7 Install & Set Up Apache Airflow </b></summary>

### Apache Airflow

<details><summary><b> 1. Install Using Terminal </b></summary>

**1. Open WSL with Terminal**:  
   - Launch WSL from your terminal or use the Ubuntu application.

**2. Navigate to Project Folder**:
   - Change directory to your project folder:
     ```bash
     cd <ProjectFolder>
     ```
   - Replace `<ProjectFolder>` with your actual project folder path.

**3. Set up a Python Virtual Environment for Airflow**:
   - Create a virtual environment:
     ```bash
     python3 -m venv airflow_env
     ```
   - Activate the virtual environment:
     ```bash
     source airflow_env/bin/activate
     ```

**4. Install Apache Airflow**:
   - Use pip to install Airflow with necessary extras:
     ```bash
     pip3 install apache-airflow[gcp,sentry,statsd]
     ```

**5. Create Airflow Directory**:
   - Create an `airflow` directory inside your project folder:
     ```bash
     mkdir airflow
     cd airflow
     ```

**6. Set up the Dynamic AIRFLOW_HOME Path**:
   - Add the following lines to your `~/.bashrc` file to set up the dynamic `AIRFLOW_HOME` path:
     ```bash
     export AIRFLOW_HOME=$(pwd)
     ```
   - Apply the changes:
     ```bash
     source ~/.bashrc
     ```

**7. Initialize the Airflow Database**:
   - Initialize the Airflow database:
     ```bash
     airflow db init
     ```

**8. Create DAGS Folder**:
   - Create a `dags` folder inside the `airflow` directory:
     ```bash
     mkdir dags
     ```

**9. Create an Admin User for Airflow**:
   - Run the following command to create an admin user (inside the `airflow_env`):
     ```bash
     airflow users create --username admin --password your_password --firstname your_first_name --lastname your_last_name --role Admin --email your_email@domain.com
     ```
   - Replace `your_password`, `your_first_name`, `your_last_name`, and `your_email@domain.com` with your own information.

**10. List Users to Verify**:
   - List all Airflow users to verify user creation:
     ```bash
     airflow users list
     ```

**11. Run the Airflow Scheduler**:
   - Start the Airflow scheduler:
     ```bash
     airflow scheduler
     ```

**12. Run the Airflow Webserver**:
   - Open a new terminal window, activate the virtual environment, and run the webserver:
     ```bash
     source airflow_env/bin/activate
     cd airflow
     airflow webserver -p 8080
     ```

**13. Access Airflow Web UI**:
   - Open any browser and go to [http://localhost:8080](http://localhost:8080/).
   - If the port is occupied, open the `airflow.cfg` file and change the webserver port number.

**14. Create a Sample Hello World DAG**:

- **Create a Python File** in the `dags` folder (e.g., `hello_world.py`):
  ```python
  from airflow import DAG
  from airflow.operators.python import PythonOperator
  from datetime import datetime

  def say_hello():
      print("Hello, World!")

  default_args = {
      'start_date': datetime(2023, 1, 1),
      'retries': 1,
  }

  with DAG(
      dag_id='hello_world',
      default_args=default_args,
      schedule_interval='@daily',
      catchup=False,
  ) as dag:
      task = PythonOperator(
          task_id='say_hello',
          python_callable=say_hello
      )

    ```

</details>

---

</details>

<details><summary><b> II.8 Install & Set Up PostgreSQL </b></summary>

### PostgreSQL

<details><summary><b> 1. Install Using Terminal </b></summary>

**1. Open WSL with Terminal**:  
   - Launch WSL from your terminal or use the Ubuntu application.

**2. Update System Packages**:
   - Update the package list:
     ```bash
     sudo apt update
     ```

**3. Install PostgreSQL**:
   - Use the following command to install PostgreSQL and additional modules:
     ```bash
     sudo apt install postgresql postgresql-contrib -y 
     ```
     

**4. Start the PostgreSQL Service**:
   - Start the PostgreSQL service:
     ```bash
     sudo systemctl start postgresql.service
     ```

**5. Switch to the PostgreSQL User**:
   - Switch to the `postgres` user to access the PostgreSQL prompt:
     ```bash
     sudo -i -u postgres 
     ```

**6. Create a New Database**:
   - Create a new database named `sammy`:
     ```bash
     createdb sammy
     ```

**7. Access PostgreSQL Shell (psql)**:
   - Launch the PostgreSQL shell:
     ```bash
     psql 
     ```
   - To exit the PostgreSQL shell, use:
     ```bash
     \q 
     ```
   - Exit from the `postgres` user:
     ```bash
     exit 
     ```

**8. Set up the PostgreSQL Path (Optional)**:

- **Add PostgreSQL Path to the Environment**:
  - Find the PostgreSQL binary path using:
    ```bash
    which psql 
    ```
  - If the binary is in `/usr/bin`, set the path by adding the following lines to your `~/.bashrc` file:
    ```bash
    export PATH=/usr/bin:$PATH 
    ```
  - Apply the changes:
    ```bash
    source ~/.bashrc 
    ```

**9. Verify PostgreSQL Installation**:
   - Verify that PostgreSQL is installed correctly by running:
     ```bash
     psql --version 
     ```

**10. Access PostgreSQL**:

- **Connect to the `sammy` Database**:
  - Switch to the `postgres` user:
    ```bash
    sudo -i -u postgres
    ```
  - Connect to the `sammy` database:
    ```bash
    psql -d sammy 
    ```
  - You can perform SQL operations here and exit using `\q`.

</details>

---

</details>

<details><summary><b> II.9 Install & Set Up Git </b></summary>

### Git

<details><summary><b> 1. Install Using Terminal </b></summary>

**1. Open WSL with Terminal**:  
   - Launch WSL from your terminal or use the Ubuntu application.

**2. Update System Packages**:
   - Update the package list:
     ```bash
     sudo apt update 
     ```

**3. Install Git**:
   - Use the following command to install Git:
     ```bash
     sudo apt install git -y 
     ```

**4. Verify Git Installation**:
   - Check if Git is installed correctly:
     ```bash
     git --version 
     ```

**5. Set up Git Configuration**:

- **Set Your Global Username**:
  - Replace `your_username` with your GitHub username:
    ```bash
    git config --global user.name "your_username" 
    ```

- **Set Your Global Email**:
  - Replace `your_email@domain.com` with your GitHub email:
    ```bash
    git config --global user.email "your_email@domain.com" 
    ```

- **Verify Git Configuration**:
  - Check the configuration settings:
    ```bash
    git config --list 
    ```

</details>

---

</details>

## III.Set Up Python Virtual Environment

<details><summary><b> 1. Clone the Git Repository </b></summary>

**1. Open WSL with Terminal**:  
   - Launch WSL from your terminal or use the Ubuntu application.

**2. Clone the Git Repository**:
   - Use the following command to clone the repository:
     ```bash
     git clone <repo_link> 
     ```

**3. Navigate to the Cloned Repository**:
   - Change directory to the cloned folder:
     ```bash
     cd <cloned_repo_folder> 
     ```

</details>

<details><summary><b> 2. Create & Activate Python Virtual Environment </b></summary>

**1. Create a Virtual Environment**:
   - Run the following command to create a virtual environment:
     ```bash
     python3 -m venv venv
     ```

**2. Activate the Virtual Environment**:
   - Activate the virtual environment:
     ```bash
     source venv/bin/activate
     ```

**3. Install Requirements**:
   - Install dependencies from `requirements.txt`:
     ```bash
     pip install -r requirements.txt
     ```

</details>

<details><summary><b> 3. Set up Environment Variables </b></summary>

**1. Check for Existing Environment Variables**:

- **Check if Environment Variables Are Set**:
  - Use `echo` to check if the following variables are set:
    ```bash
    echo $SPARK_HOME
    echo $JAVA_HOME
    echo $KAFKA_HOME
    echo $AIRFLOW_HOME
    ```
  - If any of the above variables are not set, follow the next step to add these lines to your ~/.bashrc file to make the changes persistent
    ```bash
    echo 'export SPARK_HOME=$(ls -d ~/spark/spark-* | sort -V | tail -n 1)' >> ~/.bashrc
    echo 'export PATH=$SPARK_HOME/bin:$SPARK_HOME/sbin:$PATH' >> ~/.bashrc
    echo 'export JAVA_HOME=$(update-alternatives --query java | grep "Value: " | awk "{print \$2}" | sed "s:/bin/java::")' >> ~/.bashrc
    echo 'export PATH=$JAVA_HOME/bin:$PATH' >> ~/.bashrc
    echo 'export KAFKA_HOME=/usr/bin/kafka' >> ~/.bashrc
    echo 'export PATH=$KAFKA_HOME/bin:$PATH' >> ~/.bashrc
    echo 'export AIRFLOW_HOME=$(pwd)/airflow' >> ~/.bashrc
    echo 'export PATH=$AIRFLOW_HOME/bin:$PATH' >> ~/.bashrc
    ```
  - Apply the changes to the current shell
    ```bash
    source ~/.bashrc
    ```