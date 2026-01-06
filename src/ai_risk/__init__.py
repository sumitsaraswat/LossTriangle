"""
AI Risk Detection Module
========================

NLP-powered risk detection for insurance claims.
Scans adjuster notes for hidden risks before claims are paid.

Owner: Anya (AI Engineer)
"""

from .nlp.risk_detector import RiskDetector
from .nlp.keyword_extractor import KeywordExtractor

__all__ = ["RiskDetector", "KeywordExtractor"]








