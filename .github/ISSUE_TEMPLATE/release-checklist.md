---
name: Release + deploy vX.X.X to staging
about: Steps to release and test titiler-cmr
title: Release + deploy vX.X.X to staging
labels: ''
assignees: ''

---

- [ ] Deploy the `develop` branch to the test deployment API endpoint by dispatching [CDK Deploy Dev Workflow](https://github.com/developmentseed/titiler-cmr/actions/workflows/deploy-dev.yml) using the develop branch.
- [ ] Test https://github.com/developmentseed/titiler-cmr/issues/57 with both the test endpoint and https://staging.openveda.cloud/api/titiler-cmr
- [ ] Assuming all is working, add a new header to the CHANGELOG and make a release with tag vX.X.X (note: make sure header link is working by adding a new entry at the bottom of CHANGELOG.md)
- [ ] Email the mailing list (use bcc)
- [ ] Update the `TITILER_CMR_GIT_REF` github environment variable for the [smce-staging environment](https://github.com/NASA-IMPACT/veda-deploy/settings/environments/4556936903/edit)
- [ ] Once deploy completes, retest the endpoints in https://github.com/developmentseed/titiler-cmr/issues/57

Note: At this point, if there is an issue at this point that can be resolved quickly, go ahead and do so. If there is an issue that requires more investigation and remedy, email at least a brief update that there is a known issue that is being worked on.

- [ ] Email an update to the mailing list
