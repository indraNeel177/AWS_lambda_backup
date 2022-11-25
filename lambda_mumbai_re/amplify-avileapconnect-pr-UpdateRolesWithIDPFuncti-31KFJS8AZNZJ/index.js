const response = require('cfn-response');
const aws = require('aws-sdk');
let responseData = {};
exports.handler = function(event, context) {
  try {
    let authRoleName = event.ResourceProperties.authRoleName;
    let unauthRoleName = event.ResourceProperties.unauthRoleName;
    let idpId = event.ResourceProperties.idpId;
    let promises = [];
    let authParamsJson = { 'Version': '2012-10-17','Statement': [{'Effect': 'Allow','Principal': {'Federated': 'cognito-identity.amazonaws.com'},'Action': 'sts:AssumeRoleWithWebIdentity','Condition': {'StringEquals': {'cognito-identity.amazonaws.com:aud': idpId},'ForAnyValue:StringLike': {'cognito-identity.amazonaws.com:amr': 'authenticated'}}}]};
    let unauthParamsJson = { 'Version': '2012-10-17','Statement': [{'Effect': 'Allow','Principal': {'Federated': 'cognito-identity.amazonaws.com'},'Action': 'sts:AssumeRoleWithWebIdentity','Condition': {'StringEquals': {'cognito-identity.amazonaws.com:aud': idpId},'ForAnyValue:StringLike': {'cognito-identity.amazonaws.com:amr': 'unauthenticated'}}}]};
    if (event.RequestType == 'Delete') {
        delete authParamsJson.Statement[0].Condition;
        delete unauthParamsJson.Statement[0].Condition;
        let authParams = { PolicyDocument: JSON.stringify(authParamsJson),RoleName: authRoleName};
        let unauthParams = {PolicyDocument: JSON.stringify(unauthParamsJson),RoleName: unauthRoleName};
        const iam = new aws.IAM({ apiVersion: '2010-05-08', region: event.ResourceProperties.region});
        promises.push(iam.updateAssumeRolePolicy(authParams).promise());
        promises.push(iam.updateAssumeRolePolicy(unauthParams).promise());
        Promise.all(promises)
         .then((res) => {
            console.log("delete response data" + JSON.stringify(res));
            response.send(event, context, response.SUCCESS, {});
         });
    }
    if (event.RequestType == 'Update' || event.RequestType == 'Create') {
       const iam = new aws.IAM({ apiVersion: '2010-05-08', region: event.ResourceProperties.region});
        let authParams = { PolicyDocument: JSON.stringify(authParamsJson),RoleName: authRoleName};
        let unauthParams = {PolicyDocument: JSON.stringify(unauthParamsJson),RoleName: unauthRoleName};
        promises.push(iam.updateAssumeRolePolicy(authParams).promise());
        promises.push(iam.updateAssumeRolePolicy(unauthParams).promise());
        Promise.all(promises)
         .then((res) => {
            console.log("createORupdate" + res);
            console.log("response data" + JSON.stringify(res));
            response.send(event, context, response.SUCCESS, {});
         });
    }
  } catch(err) {
       console.log(err.stack);
       responseData = {Error: err};
       response.send(event, context, response.FAILED, responseData);
       throw err;
  }
};