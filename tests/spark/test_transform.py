from unittest.mock import MagicMock


class TestLogPipelineStatus:
    def test_logs_success(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df

        from src.spark.transform import log_pipeline_status

        log_pipeline_status(mock_spark, "SUCCESS", count=42)

        mock_spark.createDataFrame.assert_called_once()
        args = mock_spark.createDataFrame.call_args[0][0]
        assert args[0]["status"] == "SUCCESS"
        assert args[0]["records_count"] == 42
        assert args[0]["error_message"] == ""
        mock_df.write.mode("append").parquet.assert_called_once()

    def test_logs_failure_with_error(self):
        mock_spark = MagicMock()
        mock_df = MagicMock()
        mock_spark.createDataFrame.return_value = mock_df

        from src.spark.transform import log_pipeline_status

        long_error = "x" * 1000
        log_pipeline_status(mock_spark, "FAILED", error=long_error)

        args = mock_spark.createDataFrame.call_args[0][0]
        assert args[0]["status"] == "FAILED"
        assert args[0]["records_count"] == 0
        assert len(args[0]["error_message"]) == 500


class TestRegexPatterns:
    def test_years_extraction(self):
        import re

        years_regex = r"(\d+)\s*(?:ans?|ann[ée]es?)(?:\s*d['\s]exp[ée]rience|\s*de\s*pratique|\s*minimum)?"
        assert re.search(years_regex, "3 ans d'expérience")
        assert re.search(years_regex, "5 années minimum")
        assert re.search(years_regex, "2 ans")
        assert not re.search(years_regex, "pas d'expérience")

    def test_months_regex(self):
        import re

        months_regex = r"(\d+)\s*mois"
        assert re.search(months_regex, "6 mois")
        assert re.search(months_regex, "12 mois")
        assert not re.search(months_regex, "1 an")

    def test_red_flag_regex(self):
        import re

        red_flag_re = r"alternance|stage|support|technicien|helpdesk"
        assert re.search(red_flag_re, "Offre de stage en Data")
        assert re.search(red_flag_re, "Contrat alternance")
        assert re.search(red_flag_re, "Poste technicien")
        assert not re.search(red_flag_re, "Data Engineer junior")

    def test_junior_regex(self):
        import re

        junior_re = r"junior|débutant|jeune diplômé"
        assert re.search(junior_re, "Data Engineer junior")
        assert re.search(junior_re, "débutant accepté")
        assert re.search(junior_re, "jeune diplômé en Data")
        assert not re.search(junior_re, "senior data engineer")

    def test_senior_regex(self):
        import re

        senior_re = r"senior|expert|lead|confirmé"
        assert re.search(senior_re, "senior data engineer")
        assert re.search(senior_re, "expert en data")
        assert re.search(senior_re, "lead data")
        assert re.search(senior_re, "data confirmé")
        assert not re.search(senior_re, "junior data")

    def test_ethics_regex_positive(self):
        import re

        ethical_re = r"impact|green|environnement|transition|coopérative|scic|scop|ess"
        assert re.search(ethical_re, "entreprise à impact")
        assert re.search(ethical_re, "environnement")
        assert re.search(ethical_re, "coopérative")
        assert not re.search(ethical_re, "banque finance")

    def test_remote_regex(self):
        import re

        remote_re = r"télétravail|remote|home\s?office|full\s?remote|distanciel"
        assert re.search(remote_re, "télétravail partiel")
        assert re.search(remote_re, "remote friendly")
        assert re.search(remote_re, "full remote")
        assert re.search(remote_re, "home office")
        assert not re.search(remote_re, "présentiel")

    def test_salary_regex(self):
        import re

        salary_regex = r"(\d{2}\s?\d{3}|\d{2}k|\d{5})"
        assert re.search(salary_regex, "45000")
        assert re.search(salary_regex, "45 000")
        assert re.search(salary_regex, "45k")
        assert not re.search(salary_regex, "1000")


class TestSparkPipeline:
    def test_create_spark_session(self):
        from src.spark.transform import create_spark_session

        spark = create_spark_session()
        assert spark is not None
        assert spark.sparkContext.appName == "JobRadar_Silver_Layer"
        spark.stop()
