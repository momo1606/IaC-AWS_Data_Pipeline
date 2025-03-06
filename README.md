

```
$ python3 -m venv .venv
```



```
$ source .venv/bin/activate
```

 Windows platform,  activate the virtualenv like this:

```
% .venv\Scripts\activate.bat
```

Once the virtualenv is activated, install the required dependencies.

```
$ pip install -r requirements.txt
```

 synthesize the CloudFormation template for this code.

```
$ cdk synth
```

To add additional dependencies, add them to  `setup.py` file and rerun the `pip install -r requirements.txt`
command.

##  commands

 * `cdk ls`          list all stacks in the app
 * `cdk synth`       emits the synthesized CloudFormation template
 * `cdk deploy`      deploy this stack to your default AWS account/region
 * `cdk diff`        compare deployed stack with current state
 * `cdk docs`        open CDK documentation

