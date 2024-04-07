#!/usr/bin/env python3
import os
# import aws_cdk as cdk
from aws_cdk import App, Environment

from term_assignment.term_assignment_stack import TermAssignmentStack

aws_env = Environment(account='143176219551', region='us-east-1')

app = App()
term_stack=TermAssignmentStack(app, "KinesisEcomStack", env=aws_env, )

app.synth()
