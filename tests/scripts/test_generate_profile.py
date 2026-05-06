import os
import tempfile
from unittest.mock import MagicMock, patch

import pandas as pd
import pytest


class TestGenerateProfile:
    def test_output_path_constant(self):
        from src.scripts.generate_profile import OUTPUT_PATH

        assert OUTPUT_PATH.endswith("ideal_profile_vector.csv")

    def test_profile_description_non_empty(self):
        from src.scripts.generate_profile import MY_PROFILE_DESC

        assert len(MY_PROFILE_DESC) > 100
        assert "Data Engineer" in MY_PROFILE_DESC
        assert "Nantes" in MY_PROFILE_DESC

    @patch("src.scripts.generate_profile.SentenceTransformer")
    def test_generates_csv_with_correct_structure(self, mock_transformer):
        mock_model = MagicMock()
        mock_transformer.return_value = mock_model
        mock_model.encode.return_value.tolist.return_value = [0.1, 0.2, 0.3, 0.4]

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "src.scripts.generate_profile.OUTPUT_PATH",
                os.path.join(tmpdir, "ideal_profile_vector.csv"),
            ):
                from src.scripts.generate_profile import generate_ideal_profile_seed

                generate_ideal_profile_seed()

                df = pd.read_csv(os.path.join(tmpdir, "ideal_profile_vector.csv"))
                assert list(df.columns) == ["profile_id", "description", "ideal_vector"]
                assert len(df) == 1
                assert df.iloc[0]["profile_id"] == "manuel_ideal_profile"
                assert "Data Engineer" in df.iloc[0]["description"]

    @patch("src.scripts.generate_profile.SentenceTransformer")
    def test_model_loaded_with_correct_name(self, mock_transformer):
        mock_transformer.return_value = MagicMock()

        with tempfile.TemporaryDirectory() as tmpdir:
            with patch(
                "src.scripts.generate_profile.OUTPUT_PATH",
                os.path.join(tmpdir, "ideal_profile_vector.csv"),
            ):
                from src.scripts.generate_profile import generate_ideal_profile_seed

                generate_ideal_profile_seed()

                mock_transformer.assert_called_with("all-MiniLM-L6-v2")

    @patch("src.scripts.generate_profile.SentenceTransformer")
    def test_model_failure_raises(self, mock_transformer):
        mock_transformer.side_effect = Exception("Model download failed")

        with pytest.raises(Exception, match="Model download failed"):
            from src.scripts.generate_profile import generate_ideal_profile_seed

            generate_ideal_profile_seed()
