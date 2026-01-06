"""
Keyword Extractor
=================

Extracts risk-related keywords from claim text fields.
Uses domain-specific vocabulary for insurance risk detection.
"""

import re
from typing import Optional
from pyspark.sql import DataFrame
from pyspark.sql.functions import (
    col, udf, lower, array, lit,
    when, size, array_distinct, concat_ws,
)
from pyspark.sql.types import ArrayType, StringType, DoubleType
from loguru import logger


# Risk keyword categories with associated severity weights
RISK_KEYWORDS = {
    # Water/Mold risks (HIGH - often lead to long-tail claims)
    "mold": {"category": "water_damage", "severity": 0.9},
    "mould": {"category": "water_damage", "severity": 0.9},
    "toxic mold": {"category": "water_damage", "severity": 1.0},
    "black mold": {"category": "water_damage", "severity": 1.0},
    "seepage": {"category": "water_damage", "severity": 0.7},
    "leak": {"category": "water_damage", "severity": 0.5},
    "leaking": {"category": "water_damage", "severity": 0.5},
    "water damage": {"category": "water_damage", "severity": 0.6},
    "flooding": {"category": "water_damage", "severity": 0.6},
    "moisture": {"category": "water_damage", "severity": 0.4},
    "damp": {"category": "water_damage", "severity": 0.4},
    "dampness": {"category": "water_damage", "severity": 0.4},
    "humidity": {"category": "water_damage", "severity": 0.3},
    "condensation": {"category": "water_damage", "severity": 0.3},
    
    # Legal/Litigation risks (HIGH - expensive)
    "attorney": {"category": "litigation", "severity": 0.9},
    "lawyer": {"category": "litigation", "severity": 0.9},
    "lawsuit": {"category": "litigation", "severity": 1.0},
    "litigation": {"category": "litigation", "severity": 1.0},
    "legal": {"category": "litigation", "severity": 0.7},
    "sue": {"category": "litigation", "severity": 0.8},
    "suing": {"category": "litigation", "severity": 0.8},
    "court": {"category": "litigation", "severity": 0.8},
    "demand letter": {"category": "litigation", "severity": 0.9},
    "representation": {"category": "litigation", "severity": 0.6},
    
    # Structural risks (MEDIUM-HIGH)
    "structural": {"category": "structural", "severity": 0.7},
    "foundation": {"category": "structural", "severity": 0.8},
    "collapse": {"category": "structural", "severity": 0.9},
    "settling": {"category": "structural", "severity": 0.6},
    "crack": {"category": "structural", "severity": 0.5},
    "cracking": {"category": "structural", "severity": 0.5},
    "sinkhole": {"category": "structural", "severity": 1.0},
    
    # Injury risks (HIGH)
    "injury": {"category": "bodily_injury", "severity": 0.8},
    "injured": {"category": "bodily_injury", "severity": 0.8},
    "medical": {"category": "bodily_injury", "severity": 0.6},
    "hospital": {"category": "bodily_injury", "severity": 0.7},
    "death": {"category": "bodily_injury", "severity": 1.0},
    "fatality": {"category": "bodily_injury", "severity": 1.0},
    
    # Fraud indicators (HIGH)
    "suspicious": {"category": "fraud", "severity": 0.7},
    "inconsistent": {"category": "fraud", "severity": 0.6},
    "contradictory": {"category": "fraud", "severity": 0.7},
    "staged": {"category": "fraud", "severity": 0.9},
    "prior damage": {"category": "fraud", "severity": 0.8},
    "pre-existing": {"category": "fraud", "severity": 0.7},
    
    # Catastrophe indicators
    "hurricane": {"category": "catastrophe", "severity": 0.5},
    "tornado": {"category": "catastrophe", "severity": 0.5},
    "earthquake": {"category": "catastrophe", "severity": 0.5},
    "wildfire": {"category": "catastrophe", "severity": 0.5},
    "hail": {"category": "catastrophe", "severity": 0.4},
}


class KeywordExtractor:
    """
    Extracts risk keywords from claim text fields.
    
    Uses a domain-specific vocabulary to identify
    potential long-tail risks in insurance claims.
    """
    
    def __init__(self, custom_keywords: Optional[dict] = None):
        """
        Initialize the keyword extractor.
        
        Args:
            custom_keywords: Additional keywords to include
        """
        self.keywords = RISK_KEYWORDS.copy()
        if custom_keywords:
            self.keywords.update(custom_keywords)
        
        # Pre-compile regex patterns for efficiency
        self._patterns = {
            kw: re.compile(rf'\b{re.escape(kw)}\b', re.IGNORECASE)
            for kw in self.keywords.keys()
        }
        
        logger.info(f"Initialized KeywordExtractor with {len(self.keywords)} keywords")
    
    def extract_keywords(self, text: str) -> list[str]:
        """
        Extract risk keywords from text.
        
        Args:
            text: Input text to analyze
            
        Returns:
            List of matched keywords
        """
        if not text:
            return []
        
        matches = []
        text_lower = text.lower()
        
        for keyword, pattern in self._patterns.items():
            if pattern.search(text_lower):
                matches.append(keyword)
        
        return matches
    
    def calculate_risk_score(self, text: str) -> float:
        """
        Calculate aggregate risk score from text.
        
        Args:
            text: Input text to analyze
            
        Returns:
            Risk score between 0 and 1
        """
        keywords = self.extract_keywords(text)
        if not keywords:
            return 0.0
        
        # Sum severities with diminishing returns
        total_severity = sum(
            self.keywords[kw]["severity"] 
            for kw in keywords
        )
        
        # Normalize to 0-1 range with saturation
        return min(1.0, total_severity / 3.0)
    
    def get_risk_category(self, text: str) -> str:
        """
        Determine primary risk category from text.
        
        Args:
            text: Input text to analyze
            
        Returns:
            Primary risk category or 'low_risk'
        """
        keywords = self.extract_keywords(text)
        if not keywords:
            return "low_risk"
        
        # Find category with highest severity
        category_scores = {}
        for kw in keywords:
            cat = self.keywords[kw]["category"]
            sev = self.keywords[kw]["severity"]
            category_scores[cat] = max(category_scores.get(cat, 0), sev)
        
        if not category_scores:
            return "low_risk"
        
        return max(category_scores.items(), key=lambda x: x[1])[0]
    
    def create_spark_udfs(self):
        """
        Create Spark UDFs for distributed processing.
        
        Returns:
            Tuple of (extract_udf, score_udf, category_udf)
        """
        # Capture instance methods for closure
        _extract = self.extract_keywords
        _score = self.calculate_risk_score
        _category = self.get_risk_category
        
        extract_udf = udf(_extract, ArrayType(StringType()))
        score_udf = udf(_score, DoubleType())
        category_udf = udf(_category, StringType())
        
        return extract_udf, score_udf, category_udf
    
    def enrich_dataframe(
        self,
        df: DataFrame,
        text_col: str = "adjuster_notes",
        include_description: bool = True,
    ) -> DataFrame:
        """
        Enrich DataFrame with risk keywords and scores.
        
        Args:
            df: Input DataFrame
            text_col: Column containing text to analyze
            include_description: Also analyze loss_description
            
        Returns:
            DataFrame with risk_keywords, risk_score, risk_category
        """
        logger.info(f"Enriching DataFrame with risk detection on '{text_col}'")
        
        extract_udf, score_udf, category_udf = self.create_spark_udfs()
        
        # Combine text fields if requested
        if include_description and "loss_description" in df.columns:
            df = df.withColumn(
                "_combined_text",
                concat_ws(" ", col(text_col), col("loss_description"))
            )
            analysis_col = "_combined_text"
        else:
            analysis_col = text_col
        
        # Apply risk detection
        df = df.withColumn("risk_keywords", extract_udf(col(analysis_col)))
        df = df.withColumn("risk_score", score_udf(col(analysis_col)))
        df = df.withColumn("risk_category", category_udf(col(analysis_col)))
        
        # Add high risk flag
        df = df.withColumn(
            "high_risk_flag",
            when(col("risk_score") >= 0.5, True).otherwise(False)
        )
        
        # Clean up temp column
        if "_combined_text" in df.columns:
            df = df.drop("_combined_text")
        
        logger.info("Risk enrichment complete")
        return df








