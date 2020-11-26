# Azure Synapse Analytics CI/CD using GitHub

## Connect Azure Synapse Workspace to Github

- Log into https://web.azuresynapse.net/
- Select your subscription
- Select the workspace
- on the home page on left menu click Manage
- Select Git Configuration
- Select Github
- Needs authentication
- Type the Repo URL github.com/username
- Select the repo - repository name
- Create a working branch called wip - work in progress
- Will create publish branch as workspace_publish
- Select Root Folder as /

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/cicd1.jpg "Synapse Analytics")

- On the top left side of canvas click the drop down to see the branch

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/cicd2.jpg "Synapse Analytics")

- From now anything developed will be stored in Github
- If you want to switch off from Github select Live mode
- Once you commit any changes, here is how the github repo looks like

![alt text](https://github.com/balakreshnan/synapseAnalytics/blob/master/images/cicd3.jpg "Synapse Analytics")