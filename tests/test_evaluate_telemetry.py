"""
Testes unitários das regras de detecção de anomalia (SmartFactoryConsumer.evaluate_telemetry).

Não exigem cluster Kafka: o construtor apenas lê variáveis de ambiente. Execução (dentro da
imagem do consumidor, com os limites de config/sensor_thresholds.env):

    make unit-test
"""

import unittest

from data_processor import SmartFactoryConsumer


class EvaluateTelemetryTest(unittest.TestCase):
    """Verifica classificação NORMAL / WARNING / CRITICAL nos limites configurados."""

    def setUp(self) -> None:
        """Cria um consumidor com os limites vindos do ambiente (sem conectar ao Kafka)."""
        self.c = SmartFactoryConsumer()

    def _leitura(self, temp: float, vib: float, kw: float) -> dict:
        """Monta uma leitura de telemetria com os valores informados."""
        return {"temperatura": temp, "vibracao": vib, "consumo_energia_kw": kw}

    def _nominal(self) -> dict:
        """Devolve uma leitura confortavelmente abaixo de todos os limites de aviso."""
        return self._leitura(
            self.c.warn_temp - 10, self.c.warn_vibration - 1, self.c.warn_power_kw - 5
        )

    def test_operacao_nominal_nao_gera_alerta(self) -> None:
        """Uma leitura nominal é NORMAL e não produz motivos."""
        severidade, motivos = self.c.evaluate_telemetry(self._nominal())
        self.assertEqual(severidade, "NORMAL")
        self.assertEqual(motivos, [])

    def test_limite_de_aviso_e_inclusivo(self) -> None:
        """Valor exatamente igual ao limite de aviso já classifica como WARNING."""
        for campo, valor in (
            ("temperatura", self.c.warn_temp),
            ("vibracao", self.c.warn_vibration),
            ("consumo_energia_kw", self.c.warn_power_kw),
        ):
            leitura = self._nominal()
            leitura[campo] = valor
            severidade, motivos = self.c.evaluate_telemetry(leitura)
            self.assertEqual(severidade, "WARNING", campo)
            self.assertEqual(len(motivos), 1, campo)

    def test_limite_critico_e_inclusivo(self) -> None:
        """Valor exatamente igual ao limite crítico já classifica como CRITICAL."""
        for campo, valor in (
            ("temperatura", self.c.max_temp),
            ("vibracao", self.c.max_vibration),
            ("consumo_energia_kw", self.c.max_power_kw),
        ):
            leitura = self._nominal()
            leitura[campo] = valor
            severidade, _ = self.c.evaluate_telemetry(leitura)
            self.assertEqual(severidade, "CRITICAL", campo)

    def test_critico_prevalece_sobre_aviso(self) -> None:
        """Quando há uma violação crítica e outra de aviso, a severidade final é CRITICAL."""
        leitura = self._leitura(self.c.max_temp + 1, self.c.warn_vibration, 0.0)
        severidade, motivos = self.c.evaluate_telemetry(leitura)
        self.assertEqual(severidade, "CRITICAL")
        self.assertEqual(len(motivos), 2)

    def test_multiplas_violacoes_geram_multiplos_motivos(self) -> None:
        """Cada grandeza violada acrescenta um motivo à lista devolvida."""
        leitura = self._leitura(
            self.c.max_temp + 1, self.c.max_vibration + 1, self.c.max_power_kw + 1
        )
        severidade, motivos = self.c.evaluate_telemetry(leitura)
        self.assertEqual(severidade, "CRITICAL")
        self.assertEqual(len(motivos), 3)


if __name__ == "__main__":
    unittest.main()
