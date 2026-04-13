"""Class that applies permissions boundary to all the roles created within a Stack"""

from typing import Union

import jsii
from aws_cdk import IAspect, aws_iam
from constructs import IConstruct
from jsii._reference_map import _refs
from jsii._utils import Singleton


@jsii.implements(IAspect)
class PermissionsBoundaryAspect:
    """
    This aspect finds all aws_iam.Role objects in a node (ie. CDK stack) and sets permissions boundary to the given ARN.
    """

    def __init__(self, permissions_boundary: Union[aws_iam.ManagedPolicy, str]) -> None:
        """
        :param permissions_boundary: Either aws_iam.ManagedPolicy object or managed policy's ARN string
        """
        self.permissions_boundary = permissions_boundary

    def visit(self, construct_ref: IConstruct) -> None:
        """
        construct_ref only contains a string reference to an object. To get the actual object, we need to resolve it using JSII mapping.
        :param construct_ref: ObjRef object with string reference to the actual object.
        :return: None
        """
        if isinstance(construct_ref, jsii._kernel.ObjRef) and hasattr(
            construct_ref, "ref"
        ):
            kernel = Singleton._instances[
                jsii._kernel.Kernel
            ]  # The same object is available as: jsii.kernel
            resolve = _refs.resolve(kernel, construct_ref)
        else:
            resolve = construct_ref

        def _walk(obj):
            if isinstance(obj, aws_iam.Role):
                cfn_role = obj.node.find_child("Resource")
                policy_arn = (
                    self.permissions_boundary
                    if isinstance(self.permissions_boundary, str)
                    else self.permissions_boundary.managed_policy_arn
                )
                cfn_role.add_property_override("PermissionsBoundary", policy_arn)
            else:
                if hasattr(obj, "permissions_node"):
                    for c in obj.permissions_node.children:
                        _walk(c)
                if hasattr(obj, "node") and obj.node.children:
                    for c in obj.node.children:
                        _walk(c)

        _walk(resolve)
