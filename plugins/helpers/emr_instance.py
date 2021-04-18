from scripts.bucket import bucketname

class InstanceEMR:
    JOB_FLOW_OVERRIDES = {
        'Name':'Capstone Cluster',
        'LogUri':'s3n://aws-logs-{}-us-west-2/elasticmapreduce/'.format(bucketname.bucket),
        'ReleaseLabel':'emr-5.30.0',
        'Applications':[{'Name':'Hadoop'}, 
                {'Name':'Spark'},{'Name':'Zeppelin'},
                {'Name':'JupyterHub'}],
        'BootstrapActions':[
                {
                        'Name':'Install Modules',
                        "ScriptBootstrapAction":{
                                "Path":"s3://{}/scripts/python-modules.sh".format(bucketname.bucket)
                                }
                        
                }
                ],
        "Configurations": [
                { "Classification": "spark-env",
                "Configurations": [
                        {
                        "Classification": "export",
                        "Properties": {
                                "PYSPARK_PYTHON": "/usr/bin/python3"
                        },
                        }
                        ],
                }
                ],
        'Instances':{
                'InstanceGroups': [
                {
                        'Name':'Master Node',
                        'Market':'ON_DEMAND',
                        'InstanceRole':'MASTER',
                        'InstanceType':'m5.xlarge',
                        'InstanceCount':1,
                },
                {
                        'Name':'Cores',
                        'Market':'ON_DEMAND',
                        'InstanceRole':'CORE',
                        'InstanceType':'m5.xlarge',
                        'InstanceCount':1,

                },
        ],
        'KeepJobFlowAliveWhenNoSteps': True,
        'TerminationProtected': False,
        },
        'JobFlowRole':'EMR_EC2_DefaultRole',
        'ServiceRole':'EMR_DefaultRole',
    } 
