The ADAP project uses containers to run PypeIt in the Nautilus
Kubernetes cluster. There are three containers, two PypeIt
containers and one workqueue container. These containers 
are pushed to the `Nautilus GitLab <https://gitlab.nrp-nautilus.io/dustyreichwein/pypeitcontainers>`_ 
so they can be quickly accessed by Naitilus jobs.

PypeIt Containers
=================

The PypeIt containers are  built from the ``pypeit`` subdirectory.
There are two containers, one for a development 
version of pypeit (``pypeit_develop.docker``) and one for a release 
version of pypeit ( ``pypeit_release.docker``. 


PypeIt Release Container
------------------------
The PypeIt release container holds a specific version of PypeIt 
and a specific version of each dependency. The version of PypeIt
is installed directly from `PyPi <https://pypi.org/project/pypeit/>`_.

The dependencies are controlled by the ``pypeit_release_requirements.txt`` file, 
which must be built as described below.

Building the Requirements file
++++++++++++++++++++++++++++++

The ``build_requirements.sh`` script will install PypeIt into a fresh
python virtual environment and then "freeze" that environment into 
a list of requirements. For example::

    $ python3 -m venv ~/work/venvs/pypeit_release
    $ ./build_requirements.sh ~/work/venvs/pypeit_release/ 1.17.4 pypeit_release_requirements.txt

**NOTE** The python version should be the same as installed in the docker container. Currently that is
Python 3.12.

After this the virtual environment can be cleaned up if desired::

    $ rm -r ~/work/venvs/pypeit_release

The PypeIt Development Container
--------------------------------
The PypeIt development container is built using the `PypeIt develop branch <https://github.com/pypeit/PypeIt/tree/develop>` 
and the latest dependencies at the time the container is built.

Jobs using this container will probably want to do a re-install of PypeIt to get the latest and greatest development code,
as described below.       

Container users and environment
-------------------------------

The PypeIt docker containers are built off of Ubuntu 24.04. They each have a ``pypeitusr`` and a ``pypeit_env`` 
virtual environment that should be used to run PypeIt. For example, the below shows the yaml file contents
to properly setup and install PypeIt in the development container::

    args:
        - source /home/pypeitusr/pypeit_env/bin/activate; pip install redis boto3 google-api-python-client gspread;
          cd /home/pypeitusr/PypeIt; git fetch; git checkout develop; export OMP_NUM_THREADS=2;
          pip install -e '.[dev]';

Redis Work Queue Container
==========================

The ADAP Nautilus scripts use `Redis <https://redis.io/docs/latest/>` to manage a job queue. 
That ``redis`` instance runs in its own container build from ``pypeit_workqueue_redis.docker`` in the ``redis``
subdirectory.

Building the Docker Containers 
===============================

Below are instructions on how to build the containers and push them to the Nautilus GitLab container registry.
**NOTE** It may be required to run ``docker`` with ``sudo`` depending on your local OS requirements.

To build the PypeIt release container::

    $ docker build -f pypeit_release.docker --tag gitlab-registry.nrp-nautilus.io/dustyreichwein/pypeitcontainers/pypeit:1.16.0 --tag gitlab-registry.nrp-nautilus.io/dustyreichwein/pypeitcontainers/pypeit:release .
    $ docker push gitlab-registry.nrp-nautilus.io/dustyreichwein/pypeitcontainers/pypeit:1.16.0

To build the PypeIt development container:    :

    $ docker build -f pypeit_release.docker --tag gitlab-registry.nrp-nautilus.io/dustyreichwein/pypeitcontainers/pypeit:develop .
    $ docker push gitlab-registry.nrp-nautilus.io/dustyreichwein/pypeitcontainers/pypeit:develop

To build the PypeIt workqueue container::

    $ docker build -f pypeit_workqueue_redis.docker --tag gitlab-registry.nrp-nautilus.io/dustyreichwein/pypeitcontainers/workqueue/pypeit_workqueue_redis:latest .
    $ docker push gitlab-registry.nrp-nautilus.io/dustyreichwein/pypeitcontainers/workqueue/pypeit_workqueue_redis:latest
