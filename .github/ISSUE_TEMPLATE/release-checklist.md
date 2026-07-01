---
name: Release + deploy vX.X.X to staging
about: Steps to release and test titiler-cmr
title: Release + deploy vX.X.X to staging
labels: ''
assignees: ''

---

- [ ] Deploy the `main` branch to the test deployment API endpoint by dispatching [CDK Deploy Dev Workflow](https://github.com/developmentseed/titiler-cmr/actions/workflows/deploy-dev.yml) using the develop branch.
- [ ] Test https://github.com/developmentseed/titiler-cmr/issues/57 with both the dev endpoint (https://v4jec6i5c0.execute-api.us-west-2.amazonaws.com) and https://staging.openveda.cloud/api/titiler-cmr
- [ ] Assuming all is working, merge the release-please PR (changes will accumulate there from other merged PRs). This will create a new tag (vX.Y.Z) and generate a new release in GitHub.
- [ ] Email the mailing list (use bcc, ask Aimee or Henry for the list of email addresses)
- [ ] Update the `TITILER_CMR_GIT_REF` github environment variable for the [smce-staging environment](https://github.com/NASA-IMPACT/veda-deploy/settings/environments/4556936903/edit) then dispatch the deployment in veda-deploy
- [ ] Once deploy completes, retest the endpoints in https://github.com/developmentseed/titiler-cmr/issues/57
- [ ] Update the `TITILER_CMR_GIT_REF` github environment variable for the [mcp-prod environment](https://github.com/NASA-IMPACT/veda-deploy/settings/environments/2525365130/edit) then dispatch the deployment in veda-deploy

Note: At this point, if there is an issue at this point that can be resolved quickly, go ahead and do so. If there is an issue that requires more investigation and remedy, email at least a brief update that there is a known issue that is being worked on.

- [ ] Email an update to the mailing list
