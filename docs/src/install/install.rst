************
Installation
************

Dependencies
############

After downloading the Opaque codebase, build and test it as follows.

1. Install dependencies and the `OpenEnclave SDK <https://github.com/openenclave/openenclave/blob/v0.12.0/docs/GettingStartedDocs/install_oe_sdk-Ubuntu_18.04.md>`_. We currently support OE version 0.12.0 (so please install with ``open-enclave=0.12.0``) and Ubuntu 18.04.

   .. code-block:: bash
               
                   # For Ubuntu 18.04:
                   sudo apt install wget build-essential openjdk-8-jdk python libssl-dev libmbedtls-dev

                   # Install a newer version of CMake (>= 3.13)
                   wget https://github.com/Kitware/CMake/releases/download/v3.15.6/cmake-3.15.6-Linux-x86_64.sh
                   sudo bash cmake-3.15.6-Linux-x86_64.sh --skip-license --prefix=/usr/local

                   # Install Spark 3.1.1 (if not already done)
                   wget https://downloads.apache.org/spark/spark-3.1.1/spark-3.1.1-bin-hadoop2.7.tgz
                   tar xvf spark-3.1.1*
                   sudo mkdir /opt/spark
                   sudo mv spark-3.1.1*/* /opt/spark
                   rm -rf spark-3.1.1*
                   sudo mkdir /opt/spark/work
                   sudo chmod -R a+wx /opt/spark/work

                   # Set Spark environment variables in ~/.bashrc
                   echo "" >> ~/.bashrc
                   echo "# Spark settings" >> ~/.bashrc
                   echo "export SPARK_HOME=/opt/spark" >> ~/.bashrc
                   echo "export PATH=$PATH:/opt/spark/bin:/opt/spark/sbin" >> ~/.bashrc
                   source ~/.bashrc

2. Change into the Opaque root directory and edit Opaque's environment variables in ``opaqueenv`` if desired. Export Opaque and OpenEnclave environment variables via

   .. code-block:: bash
                   
                   source opaqueenv
                   source /opt/openenclave/share/openenclave/openenclaverc

   By default, Opaque runs in hardware mode (environment variable ``MODE=HARDWARE``).
   If you do not have a machine with a hardware enclave but still wish to test out Opaque's functionality locally, then set ``export MODE=SIMULATE``.

3. To generate new keys using OpenSSL, you can use the following ``sbt`` task:

   .. code-block:: bash

                  build/sbt keys

   *Note that, by default, Opaque SQL uses a pre-generated RSA private key for enclave signing. This key should be not be used in a production environment.* 

   Alternatively, you can use your own keys, though this is generally not recommended. 

   To generate and set the private key used for remote attestation:

   .. code-block:: bash

                  openssl genrsa -out /path/to/private/key/private_key.pem -3 3072
                  export PRIVATE_KEY_PATH=/path/to/private/key/private_key.pem

   To generate and set the symmetric key used for encrypting/decrypting between the driver and the enclave:

   .. code-block:: bash

                  openssl rand -out /path/to/symmetric/key/shared_key.key 32
                  export SYMMETRIC_KEY_PATH=/path/to/symmetric/key/symmetric_key.key

4. Run the Opaque tests:

   .. code-block:: bash
                
                   build/sbt test

5. Alternatively, to generate coverage reports:

   .. code-block:: bash

                  build/sbt clean coverage test
                  build/sbt coverageReport


Additional configurations for running on a Spark cluster
########################################################

Opaque SQL needs three Spark properties to be set:

- ``spark.executor.instances=n`` (n is usually the number of machines in the cluster)
- ``spark.task.maxFailures=10`` (attestation uses Spark's fault tolerance property)
- ``spark.driver.defaultJavaOptions="-Dscala.color"`` (if querying with MC\ :sup:`2` Client)

These properties can be be set in a custom configuration file, the default being located at ``${SPARK_HOME}/conf/spark-defaults.conf``, or as a ``spark-submit`` or ``spark-shell`` argument: ``--conf <key>=<value>``. For more details on running a Spark cluster, see the `Spark documentation <https://spark.apache.org/docs/latest/cluster-overview.html>`_
