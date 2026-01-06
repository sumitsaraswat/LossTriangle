"""
Silver Layer - Triangle Transformation
======================================

Transforms raw data into actuarial loss triangles.
Handles the complex pivot from linear data to 2D triangle matrix.
"""

from .triangle_builder import TriangleBuilder
from .transformations import TriangleTransformations

__all__ = ["TriangleBuilder", "TriangleTransformations"]








