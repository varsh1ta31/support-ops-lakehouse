"""Deterministic synthetic support-data generation."""

from support_ops.synthetic.config import Distribution, GenerationConfig, load_generation_config
from support_ops.synthetic.generate import generate_dataset

__all__ = ["Distribution", "GenerationConfig", "generate_dataset", "load_generation_config"]
