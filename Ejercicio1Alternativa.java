import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.time.LocalDateTime;
import java.time.format.DateTimeFormatter;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;

/* 
 * Trabajo: Paralelismo orientado a tareas. 
 * 
 * Dataset: 
 * Mediciones horarias del consumo eléctrico de un edificio. 
 * 
 * Restricción importante: 
 * El dataset se genera y se mantiene en disco. 
 * Durante el análisis se lee línea por línea, sin cargar el CSV completo 
 * en memoria. 
 * 
 * Grafo de dependencias: 
 * 
 * A -+ 
 * +-> C -+-> D -> F -+ 
 * B -+ | +-> H 
 * +-> E -> G -+ 
 */
public class Ejercicio1Alternativa {
	private static final int REGISTROS_POR_DEFECTO = 500_000;
	private static final int NUMERO_HILOS = 2;
	private static final Path DIRECTORIO = Path.of("datos-ejercicio-1");
	private static final Path DATASET = DIRECTORIO.resolve("consumo_edificio.csv");
	private static final Path REPORTE_SERIAL = DIRECTORIO.resolve("reporte_serial.txt");
	private static final Path REPORTE_PARALELO = DIRECTORIO.resolve("reporte_paralelo.txt");

	public static void main(String[] args) {
		int totalRegistros = obtenerCantidadRegistros(args);
		try {
			Files.createDirectories(DIRECTORIO);
			System.out.println("==============================================");
			System.out.println("PARALELISMO ORIENTADO A TAREAS");
			System.out.println("==============================================");
			System.out.println("Registros a generar: " + totalRegistros);
			System.out.println("Archivo dataset: " + DATASET.toAbsolutePath());
			System.out.println();
			long inicioGeneracion = System.nanoTime();
			GeneradorDataset.generar(DATASET, totalRegistros);
			double tiempoGeneracionMs = convertirAMilisegundos(System.nanoTime() - inicioGeneracion);
			System.out.printf(
					Locale.US,
					"Dataset generado correctamente en %.3f ms.%n%n",
					tiempoGeneracionMs);
			System.out.println("----- EJECUCIÓN SERIAL -----");
			long inicioSerial = System.nanoTime();
			ResultadoFinal resultadoSerial = FlujoAnalisis.ejecutarSerial(DATASET);
			double tiempoSerialMs = convertirAMilisegundos(System.nanoTime() - inicioSerial);
			EscritorReporte.escribir(REPORTE_SERIAL, resultadoSerial, "SERIAL", tiempoSerialMs);
			System.out.println();
			System.out.println("----- EJECUCIÓN PARALELA -----");
			long inicioParalelo = System.nanoTime();
			ResultadoFinal resultadoParalelo = FlujoAnalisis.ejecutarParalelo(DATASET);
			double tiempoParaleloMs = convertirAMilisegundos(System.nanoTime() - inicioParalelo);
			EscritorReporte.escribir(
					REPORTE_PARALELO,
					resultadoParalelo,
					"PARALELO",
					tiempoParaleloMs);
			double speedup = tiempoSerialMs / tiempoParaleloMs;
			double eficiencia = (speedup / NUMERO_HILOS) * 100.0;
			System.out.println();
			System.out.println("==============================================");
			System.out.println("RESULTADOS DE RENDIMIENTO");
			System.out.println("==============================================");
			System.out.printf(Locale.US, "Tiempo serial: %.3f ms%n", tiempoSerialMs);
			System.out.printf(Locale.US, "Tiempo paralelo: %.3f ms%n", tiempoParaleloMs);
			System.out.printf(Locale.US, "Speedup: %.3f%n", speedup);
			System.out.printf(Locale.US, "Eficiencia: %.2f%%%n", eficiencia);
			System.out.println();
			System.out.println("Reportes generados:");
			System.out.println("- " + REPORTE_SERIAL.toAbsolutePath());
			System.out.println("- " + REPORTE_PARALELO.toAbsolutePath());
		} catch (IOException error) {
			System.err.println("Error de entrada/salida: " + error.getMessage());
		} catch (InterruptedException error) {
			Thread.currentThread().interrupt();
			System.err.println("La ejecución fue interrumpida.");
		}
	}

	private static int obtenerCantidadRegistros(String[] args) {
		if (args.length == 0) {
			return REGISTROS_POR_DEFECTO;
		}
		try {
			int cantidad = Integer.parseInt(args[0]);
			if (cantidad < 1) {
				throw new IllegalArgumentException(
						"La cantidad de registros debe ser mayor que cero.");
			}
			return cantidad;
		} catch (NumberFormatException error) {
			throw new IllegalArgumentException(
					"El argumento debe ser un número entero válido.",
					error);
		}
	}

	private static double convertirAMilisegundos(long nanosegundos) {
		return nanosegundos / 1_000_000.0;
	}

	/**
	 * Genera el dataset directamente en disco.
	 * No se crea una lista con todos los registros en memoria.
	 */
	static class GeneradorDataset {
		private static final DateTimeFormatter FORMATO_FECHA = DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

		private GeneradorDataset() {
		}

		static void generar(Path archivo, int totalRegistros) throws IOException {
			Random random = new Random(20261015L);
			try (BufferedWriter escritor = Files.newBufferedWriter(
					archivo,
					StandardCharsets.UTF_8,
					StandardOpenOption.CREATE,
					StandardOpenOption.TRUNCATE_EXISTING,
					StandardOpenOption.WRITE)) {
				escritor.write(
						"fechaHora,temperaturaCelsius,humedadPorcentaje,"
								+ "personas,equiposEncendidos,consumoKwh");
				escritor.newLine();
				LocalDateTime fechaInicial = LocalDateTime.of(2026, 1, 1, 0, 0);
				for (int indice = 0; indice < totalRegistros; indice++) {
					LocalDateTime fechaHora = fechaInicial.plusHours(indice);
					int hora = fechaHora.getHour();
					boolean horarioLaboral = hora >= 8 && hora <= 18;
					double temperatura = calcularTemperatura(hora, random);
					double humedad = calcularHumedad(temperatura, random);
					int personas = calcularPersonas(horarioLaboral, random);
					int equiposEncendidos = calcularEquipos(personas, random);
					double consumoKwh = calcularConsumo(
							temperatura,
							humedad,
							personas,
							equiposEncendidos,
							random);
					String fila = String.format(
							Locale.US,
							"%s,%.2f,%.2f,%d,%d,%.2f",
							fechaHora.format(FORMATO_FECHA),
							temperatura,
							humedad,
							personas,
							equiposEncendidos,
							consumoKwh);
					escritor.write(fila);
					escritor.newLine();
				}
			}
		}

		private static double calcularTemperatura(int hora, Random random) {
			double cicloDiario = 7.0 * Math.sin((hora - 8) * Math.PI / 12.0);
			double ruido = random.nextGaussian() * 1.2;
			return limitar(22.0 + cicloDiario + ruido, 12.0, 38.0);
		}

		private static double calcularHumedad(double temperatura, Random random) {
			double ruido = random.nextGaussian() * 4.0;
			return limitar(82.0 - temperatura * 1.1 + ruido, 30.0, 95.0);
		}

		private static int calcularPersonas(boolean horarioLaboral, Random random) {
			int base = horarioLaboral ? 130 : 12;
			int variacion = random.nextInt(41) - 20;
			return Math.max(0, base + variacion);
		}

		private static int calcularEquipos(int personas, Random random) {
			int variacion = random.nextInt(21) - 10;
			return Math.max(10, 25 + (int) (personas * 0.55) + variacion);
		}

		private static double calcularConsumo(
				double temperatura,
				double humedad,
				int personas,
				int equiposEncendidos,
				Random random) {
			double consumoBase = 25.0;
			double efectoTemperatura = temperatura * 1.80;
			double efectoHumedad = humedad * 0.10;
			double efectoPersonas = personas * 0.30;
			double efectoEquipos = equiposEncendidos * 0.70;
			double ruido = random.nextGaussian() * 5.0;
			return Math.max(
					0.0,
					consumoBase
							+ efectoTemperatura
							+ efectoHumedad
							+ efectoPersonas
							+ efectoEquipos
							+ ruido);
		}

		private static double limitar(double valor, double minimo, double maximo) {
			return Math.max(minimo, Math.min(valor, maximo));
		}
	}

	/**
	 * Ejecuta los nodos A-H, tanto de forma serial como paralela.
	 */
	static class FlujoAnalisis {
		private FlujoAnalisis() {
		}

		static ResultadoFinal ejecutarSerial(Path dataset) {
			ResultadoCorrelacion resultadoA = ejecutarNodo(
					"A",
					() -> AnalizadorDataset.calcularCorrelacion(
							dataset,
							Columna.TEMPERATURA,
							Columna.CONSUMO));
			ResultadoCorrelacion resultadoB = ejecutarNodo(
					"B",
					() -> AnalizadorDataset.calcularCorrelacion(
							dataset,
							Columna.PERSONAS,
							Columna.CONSUMO));
			ContextoAnalisis resultadoC = ejecutarNodo(
					"C",
					() -> construirContexto(resultadoA, resultadoB));
			ResultadoRama resultadoD = ejecutarNodo(
					"D",
					() -> analizarRama(
							dataset,
							Columna.EQUIPOS,
							resultadoC));
			ResultadoRama resultadoE = ejecutarNodo(
					"E",
					() -> analizarRama(
							dataset,
							Columna.HUMEDAD,
							resultadoC));
			String resultadoF = ejecutarNodo(
					"F",
					() -> generarRecomendacionEquipos(resultadoD));
			String resultadoG = ejecutarNodo(
					"G",
					() -> generarRecomendacionHumedad(resultadoE));
			return ejecutarNodo(
					"H",
					() -> new ResultadoFinal(
							resultadoC,
							resultadoD,
							resultadoE,
							resultadoF,
							resultadoG));
		}

		static ResultadoFinal ejecutarParalelo(Path dataset) throws InterruptedException {
			ExecutorService executor = Executors.newFixedThreadPool(NUMERO_HILOS);
			try {
				CompletableFuture<ResultadoCorrelacion> procesoA = CompletableFuture.supplyAsync(
						() -> ejecutarNodo(
								"A",
								() -> AnalizadorDataset.calcularCorrelacion(dataset,
										Columna.TEMPERATURA,
										Columna.CONSUMO)),
						executor);
				CompletableFuture<ResultadoCorrelacion> procesoB = CompletableFuture.supplyAsync(
						() -> ejecutarNodo(
								"B",
								() -> AnalizadorDataset.calcularCorrelacion(dataset,
										Columna.PERSONAS,
										Columna.CONSUMO)),
						executor);
				CompletableFuture<ContextoAnalisis> procesoC = procesoA.thenCombineAsync(
						procesoB,
						(resultadoA, resultadoB) -> ejecutarNodo(
								"C",
								() -> construirContexto(resultadoA, resultadoB)),
						executor);
				CompletableFuture<ResultadoRama> procesoD = procesoC.thenApplyAsync(
						contexto -> ejecutarNodo(
								"D",
								() -> analizarRama(
										dataset,
										Columna.EQUIPOS,
										contexto)),
						executor);
				CompletableFuture<ResultadoRama> procesoE = procesoC.thenApplyAsync(
						contexto -> ejecutarNodo(
								"E",
								() -> analizarRama(
										dataset,
										Columna.HUMEDAD,
										contexto)),
						executor);
				CompletableFuture<String> procesoF = procesoD.thenApplyAsync(
						resultadoD -> ejecutarNodo(
								"F",
								() -> generarRecomendacionEquipos(resultadoD)),
						executor);
				CompletableFuture<String> procesoG = procesoE.thenApplyAsync(
						resultadoE -> ejecutarNodo(
								"G",
								() -> generarRecomendacionHumedad(resultadoE)),
						executor);
				CompletableFuture<ResultadoFinal> procesoH = procesoF.thenCombineAsync(
						procesoG,
						(resultadoF, resultadoG) -> ejecutarNodo(
								"H",
								() -> new ResultadoFinal(
										procesoC.join(),
										procesoD.join(),
										procesoE.join(),
										resultadoF,
										resultadoG)),
						executor);
				return procesoH.join();
			} finally {
				executor.shutdown();
				executor.awaitTermination(1, TimeUnit.MINUTES);
			}
		}

		/*
		 * Nodo C:
		 * Compara los resultados de A y B e identifica qué variable
		 * tiene mayor relación lineal con el consumo eléctrico.
		 */
		private static ContextoAnalisis construirContexto(
				ResultadoCorrelacion temperaturaConsumo,
				ResultadoCorrelacion personasConsumo) {
			ResultadoCorrelacion variablePrincipal;
			if (Math.abs(temperaturaConsumo.getCoeficiente()) >= Math.abs(personasConsumo.getCoeficiente())) {
				variablePrincipal = temperaturaConsumo;
			} else {
				variablePrincipal = personasConsumo;
			}
			return new ContextoAnalisis(
					temperaturaConsumo,
					personasConsumo,
					variablePrincipal);
		}

		/*
		 * 
		 * Nodos D y E:
		 * Analizan una variable adicional, luego de que C determinó
		 * cuál era el principal factor inicial.
		 */
		private static ResultadoRama analizarRama(
				Path dataset,
				Columna variable,
				ContextoAnalisis contexto) throws IOException {
			ResultadoCorrelacion correlacion = AnalizadorDataset.calcularCorrelacion(
					dataset,
					variable,
					Columna.CONSUMO);
			return new ResultadoRama(
					correlacion,
					contexto.getVariablePrincipal().getNombreVariableX());
		}

		private static String generarRecomendacionEquipos(ResultadoRama resultadoD) {
			double correlacion = Math.abs(
					resultadoD.getCorrelacion().getCoeficiente());
			if (correlacion >= 0.70) {
				return "Los equipos encendidos presentan una relación fuerte con "
						+ "el consumo. Se recomienda programar el apagado automático "
						+ "de equipos no esenciales fuera del horario de trabajo.";
			}
			return "La influencia de los equipos encendidos es moderada o baja. "
					+ "Se recomienda revisar el consumo de los equipos de mayor potencia.";
		}

		private static String generarRecomendacionHumedad(ResultadoRama resultadoE) {
			double correlacion = Math.abs(
					resultadoE.getCorrelacion().getCoeficiente());
			if (correlacion >= 0.50) {
				return "La humedad presenta una influencia importante. Se recomienda "
						+ "optimizar la ventilación y el control de humedad del edificio.";
			}
			return "La humedad tiene una influencia secundaria sobre el consumo. "
					+ "La prioridad debe mantenerse en temperatura, ocupación y equipos.";
		}

		private static <T> T ejecutarNodo(String nodo, TareaConResultado<T> tarea) {

			long inicio = System.nanoTime();
			try {
				return tarea.ejecutar();
			} catch (IOException error) {
				throw new UncheckedIOException(error);
			} finally {
				double duracionMs = (System.nanoTime() - inicio) / 1_000_000.0;
				System.out.printf(
						Locale.US,
						"%s | %-20s | %.3f ms%n",
						nodo,
						Thread.currentThread().getName(),
						duracionMs);
			}
		}
	}

	/**
	 * Lee el CSV de forma secuencial y calcula correlación de Pearson.
	 * Solo mantiene acumuladores estadísticos en memoria.
	 */
	static class AnalizadorDataset {
		private AnalizadorDataset() {
		}

		static ResultadoCorrelacion calcularCorrelacion(
				Path archivo,
				Columna columnaX,
				Columna columnaY) throws IOException {
			AcumuladorCorrelacion acumulador = new AcumuladorCorrelacion();
			try (BufferedReader lector = Files.newBufferedReader(
					archivo,
					StandardCharsets.UTF_8)) {
				lector.readLine();
				String linea;
				while ((linea = lector.readLine()) != null) {
					String[] valores = linea.split(",");
					double valorX = Double.parseDouble(
							valores[columnaX.getIndiceCsv()]);

					double valorY = Double.parseDouble(
							valores[columnaY.getIndiceCsv()]);
					acumulador.agregar(valorX, valorY);
				}
			}
			return new ResultadoCorrelacion(
					columnaX.getNombre(),
					columnaY.getNombre(),
					acumulador.calcularCoeficientePearson(),
					acumulador.getCantidadRegistros());
		}
	}

	/**
	 * Mantiene únicamente los valores necesarios para calcular
	 * la correlación de Pearson.
	 */
	static class AcumuladorCorrelacion {
		private long cantidadRegistros;
		private double sumaX;
		private double sumaY;
		private double sumaXCuadrado;
		private double sumaYCuadrado;
		private double sumaXY;

		void agregar(double valorX, double valorY) {
			cantidadRegistros++;
			sumaX += valorX;
			sumaY += valorY;
			sumaXCuadrado += valorX * valorX;
			sumaYCuadrado += valorY * valorY;
			sumaXY += valorX * valorY;
		}

		double calcularCoeficientePearson() {
			double numerador = cantidadRegistros * sumaXY - sumaX * sumaY;
			double parteX = cantidadRegistros * sumaXCuadrado - sumaX * sumaX;
			double parteY = cantidadRegistros * sumaYCuadrado - sumaY * sumaY;
			double denominador = Math.sqrt(parteX * parteY);
			if (denominador == 0.0) {

				return 0.0;
			}
			return numerador / denominador;
		}

		long getCantidadRegistros() {
			return cantidadRegistros;
		}
	}

	static class EscritorReporte {
		private EscritorReporte() {
		}

		static void escribir(
				Path archivo,
				ResultadoFinal resultado,
				String tipoEjecucion,
				double tiempoMs) throws IOException {
			try (BufferedWriter escritor = Files.newBufferedWriter(
					archivo,
					StandardCharsets.UTF_8,
					StandardOpenOption.CREATE,
					StandardOpenOption.TRUNCATE_EXISTING,
					StandardOpenOption.WRITE)) {
				escritor.write("REPORTE DE ANÁLISIS DE CONSUMO ELÉCTRICO");
				escritor.newLine();
				escritor.write("Tipo de ejecución: " + tipoEjecucion);
				escritor.newLine();
				escritor.write(String.format(
						Locale.US,
						"Tiempo total: %.3f ms",
						tiempoMs));
				escritor.newLine();
				escritor.newLine();
				escritor.write("Nodo A - Temperatura vs consumo:");
				escritor.newLine();
				escribirCorrelacion(
						escritor,
						resultado.getContexto().getTemperaturaConsumo());
				escritor.newLine();
				escritor.write("Nodo B - Personas vs consumo:");
				escritor.newLine();
				escribirCorrelacion(

						escritor,
						resultado.getContexto().getPersonasConsumo());
				escritor.newLine();
				escritor.write("Nodo C - Variable principal detectada:");
				escritor.newLine();
				escritor.write(
						resultado.getContexto()
								.getVariablePrincipal()
								.getNombreVariableX());
				escritor.newLine();
				escritor.newLine();
				escritor.write("Nodo D - Equipos encendidos vs consumo:");
				escritor.newLine();
				escribirCorrelacion(
						escritor,
						resultado.getResultadoEquipos().getCorrelacion());
				escritor.newLine();
				escritor.write("Nodo E - Humedad vs consumo:");
				escritor.newLine();
				escribirCorrelacion(
						escritor,
						resultado.getResultadoHumedad().getCorrelacion());
				escritor.newLine();
				escritor.write("Nodo F - Recomendación sobre equipos:");
				escritor.newLine();
				escritor.write(resultado.getRecomendacionEquipos());
				escritor.newLine();
				escritor.newLine();
				escritor.write("Nodo G - Recomendación sobre humedad:");
				escritor.newLine();
				escritor.write(resultado.getRecomendacionHumedad());
				escritor.newLine();
				escritor.newLine();
				escritor.write("Nodo H - Conclusión final:");
				escritor.newLine();
				escritor.write(
						"El análisis se realizó leyendo el dataset desde disco "
								+ "línea por línea. No se almacenó el dataset " + "completo en memoria.");
				escritor.newLine();
			}

		}

		private static void escribirCorrelacion(
				BufferedWriter escritor,
				ResultadoCorrelacion resultado) throws IOException {
			escritor.write(String.format(
					Locale.US,
					"Correlación Pearson (%s, %s): %.4f",
					resultado.getNombreVariableX(),
					resultado.getNombreVariableY(),
					resultado.getCoeficiente()));
			escritor.newLine();
			escritor.write(
					"Interpretación: "
							+ interpretarCorrelacion(resultado.getCoeficiente()));
			escritor.newLine();
		}

		private static String interpretarCorrelacion(double coeficiente) {
			double valorAbsoluto = Math.abs(coeficiente);
			if (valorAbsoluto >= 0.80) {
				return "relación lineal muy fuerte";
			}
			if (valorAbsoluto >= 0.60) {
				return "relación lineal fuerte";
			}
			if (valorAbsoluto >= 0.40) {
				return "relación lineal moderada";
			}
			if (valorAbsoluto >= 0.20) {
				return "relación lineal débil";
			}
			return "relación lineal muy débil";
		}
	}

	enum Columna {
		TEMPERATURA(1, "temperaturaCelsius"),
		HUMEDAD(2, "humedadPorcentaje"),
		PERSONAS(3, "personas"),
		EQUIPOS(4, "equiposEncendidos"),
		CONSUMO(5, "consumoKwh");

		private final int indiceCsv;
		private final String nombre;

		Columna(int indiceCsv, String nombre) {
			this.indiceCsv = indiceCsv;
			this.nombre = nombre;
		}

		int getIndiceCsv() {
			return indiceCsv;
		}

		String getNombre() {
			return nombre;
		}
	}

	static class ResultadoCorrelacion {
		private final String nombreVariableX;
		private final String nombreVariableY;
		private final double coeficiente;
		private final long cantidadRegistros;

		ResultadoCorrelacion(
				String nombreVariableX,
				String nombreVariableY,
				double coeficiente,
				long cantidadRegistros) {
			this.nombreVariableX = nombreVariableX;
			this.nombreVariableY = nombreVariableY;
			this.coeficiente = coeficiente;
			this.cantidadRegistros = cantidadRegistros;
		}

		String getNombreVariableX() {
			return nombreVariableX;
		}

		String getNombreVariableY() {
			return nombreVariableY;
		}

		double getCoeficiente() {
			return coeficiente;
		}

		long getCantidadRegistros() {
			return cantidadRegistros;
		}
	}

	static class ContextoAnalisis {
		private final ResultadoCorrelacion temperaturaConsumo;
		private final ResultadoCorrelacion personasConsumo;
		private final ResultadoCorrelacion variablePrincipal;

		ContextoAnalisis(
				ResultadoCorrelacion temperaturaConsumo,
				ResultadoCorrelacion personasConsumo,
				ResultadoCorrelacion variablePrincipal) {
			this.temperaturaConsumo = temperaturaConsumo;
			this.personasConsumo = personasConsumo;
			this.variablePrincipal = variablePrincipal;
		}

		ResultadoCorrelacion getTemperaturaConsumo() {
			return temperaturaConsumo;
		}

		ResultadoCorrelacion getPersonasConsumo() {
			return personasConsumo;
		}

		ResultadoCorrelacion getVariablePrincipal() {
			return variablePrincipal;
		}
	}

	static class ResultadoRama {
		private final ResultadoCorrelacion correlacion;
		private final String variablePrincipalAnterior;

		ResultadoRama(
				ResultadoCorrelacion correlacion,
				String variablePrincipalAnterior) {
			this.correlacion = correlacion;
			this.variablePrincipalAnterior = variablePrincipalAnterior;
		}

		ResultadoCorrelacion getCorrelacion() {
			return correlacion;
		}

		String getVariablePrincipalAnterior() {
			return variablePrincipalAnterior;
		}
	}

	static class ResultadoFinal {
		private final ContextoAnalisis contexto;
		private final ResultadoRama resultadoEquipos;
		private final ResultadoRama resultadoHumedad;
		private final String recomendacionEquipos;
		private final String recomendacionHumedad;

		ResultadoFinal(
				ContextoAnalisis contexto,
				ResultadoRama resultadoEquipos,
				ResultadoRama resultadoHumedad,
				String recomendacionEquipos,
				String recomendacionHumedad) {
			this.contexto = contexto;
			this.resultadoEquipos = resultadoEquipos;
			this.resultadoHumedad = resultadoHumedad;
			this.recomendacionEquipos = recomendacionEquipos;
			this.recomendacionHumedad = recomendacionHumedad;
		}

		ContextoAnalisis getContexto() {
			return contexto;
		}

		ResultadoRama getResultadoEquipos() {
			return resultadoEquipos;
		}

		ResultadoRama getResultadoHumedad() {
			return resultadoHumedad;
		}

		String getRecomendacionEquipos() {
			return recomendacionEquipos;
		}

		String getRecomendacionHumedad() {
			return recomendacionHumedad;
		}
	}

	@FunctionalInterface
	interface TareaConResultado<T> {
		T ejecutar() throws IOException;
	}
}