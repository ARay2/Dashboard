# -*- coding: utf-8 -*-
"""
Created on Mon Apr  8 14:36:13 2019

@author: ayelita.ray
"""
from __future__ import print_function
import os
import shutil

from oauth2client import client
from oauth2client import tools
from oauth2client.file import Storage

try:
    import argparse
    flags = argparse.ArgumentParser(parents=[tools.argparser]).parse_args()
except ImportError:
    flags = None


class Auth:

    def __init__(self,
                 scopes,
                 client_secret_file,
                 application_name):
        self.SCOPES = scopes
        self.CLIENT_SECRET_FILE = client_secret_file
        self.APPLICATION_NAME = application_name

    def get_credentials(self):
        """Gets valid user credentials from storage.
        If nothing has been stored, or if the stored credentials are invalid,
        the OAuth2 flow is completed to obtain the new credentials.
        Returns:
            Credentials, the obtained credential.
        """
        cwd_dir = os.getcwd()
        credential_dir = os.path.join(cwd_dir, '.credentials')
        lambda_credential_dir = "/tmp/.credentials"

        credential_path = os.path.join(credential_dir,
                                       'google-drive-python-credentials.json')
        lambda_credential_path = os.path.join(lambda_credential_dir,
                                              'google-drive-python-credentials.json')
        if not os.path.exists(lambda_credential_dir):
            os.makedirs(lambda_credential_dir)

            shutil.copy2(credential_path,
                         lambda_credential_path)

            credential_path = lambda_credential_path
        else:
            # Folder already created and copied, just point to that now
            credential_path = lambda_credential_path

        store = Storage(credential_path)
        credentials = store.get()
        if not credentials or credentials.invalid:
            flow = client.flow_from_clientsecrets(self.CLIENT_SECRET_FILE, self.SCOPES)
            flow.user_agent = self.APPLICATION_NAME
            # if flags:
            credentials = tools.run_flow(flow, store, flags)
            # else: # Needed only for compatibility with Python 2.6
            #     credentials = tools.run(flow, store)

            print('Storing credentials to ' + credential_path)
        return credentials
