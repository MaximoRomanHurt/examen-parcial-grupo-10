import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;

// Ejecuta los procesos de archivos en serie y con dependencias paralelas.
class Main {
	private static final int CANTIDAD_POR_DEFECTO = 500_000;
	static final int HILOS = 2;

	public static void main(String[] args) throws IOException {
		int cantidad = args.length == 0 ? CANTIDAD_POR_DEFECTO : Integer.parseInt(args[0]);
		if (cantidad < 1) {
			throw new IllegalArgumentException("N debe ser mayor que cero");
		}
		Files.createDirectories(Path.of("datos-ejercicio-1"));

		long inicioSerial = System.nanoTime();
		Ejecuciones.serial(cantidad);
		long tiempoSerial = System.nanoTime() - inicioSerial;

		long inicioParalelo = System.nanoTime();
		Ejecuciones.paralela(cantidad);
		long tiempoParalelo = System.nanoTime() - inicioParalelo;

		double serialMs = tiempoSerial / 1_000_000.0;
		double paraleloMs = tiempoParalelo / 1_000_000.0;
		double speedup = (double) tiempoSerial / tiempoParalelo;
		double eficiencia = speedup / HILOS * 100.0;
		System.out.printf("Tiempo serial: %.3f ms%n", serialMs);
		System.out.printf("Tiempo paralelo: %.3f ms%n", paraleloMs);
		System.out.printf("Speedup: %.3f%n", speedup);
		System.out.printf("Eficiencia: %.2f%%%n", eficiencia);
	}
}

// Realiza cada operación usando lectura y escritura secuencial de archivos.
class ProcesosArchivos {
	private static final Path DIRECTORIO = Path.of("datos-ejercicio-1");
	private static final Path ARCHIVO_A = DIRECTORIO.resolve("datos_A.txt");
	private static final Path ARCHIVO_B = DIRECTORIO.resolve("datos_B.txt");
	private static final Path ARCHIVO_C = DIRECTORIO.resolve("C_union.txt");
	private static final Path ARCHIVO_D = DIRECTORIO.resolve("D_mayusculas.txt");
	private static final Path ARCHIVO_E = DIRECTORIO.resolve("E_filtrado.txt");
	private static final Path ARCHIVO_F = DIRECTORIO.resolve("F_sin_duplicados.txt");
	private static final Path ARCHIVO_G = DIRECTORIO.resolve("G_reporte.txt");
	private static final Path ARCHIVO_H = DIRECTORIO.resolve("H_resultado.txt");

	static void procesoA(int cantidad) throws IOException {
		generar(ARCHIVO_A, "A", cantidad);
	}

	static void procesoB(int cantidad) throws IOException {
		generar(ARCHIVO_B, "B", cantidad);
	}

	static void procesoC() throws IOException {
		try (BufferedReader lectorA = Files.newBufferedReader(ARCHIVO_A, StandardCharsets.UTF_8);
			 BufferedReader lectorB = Files.newBufferedReader(ARCHIVO_B, StandardCharsets.UTF_8);
			 BufferedWriter escritor = crearEscritor(ARCHIVO_C)) {
			copiarLineas(lectorA, escritor);
			copiarLineas(lectorB, escritor);
		}
	}

	static void procesoD() throws IOException {
		try (BufferedReader lector = Files.newBufferedReader(ARCHIVO_C, StandardCharsets.UTF_8);
			 BufferedWriter escritor = crearEscritor(ARCHIVO_D)) {
			String linea;
			while ((linea = lector.readLine()) != null) {
				escritor.write(linea.toUpperCase());
				escritor.newLine();
			}
		}
	}

	static void procesoE() throws IOException {
		try (BufferedReader lector = Files.newBufferedReader(ARCHIVO_C, StandardCharsets.UTF_8);
			 BufferedWriter escritor = crearEscritor(ARCHIVO_E)) {
			String linea;
			while ((linea = lector.readLine()) != null) {
				if (linea.contains("5")) {
					escritor.write(linea);
					escritor.newLine();
				}
			}
		}
	}

	static void procesoF() throws IOException {
		try (BufferedReader lector = Files.newBufferedReader(ARCHIVO_D, StandardCharsets.UTF_8);
			 BufferedWriter escritor = crearEscritor(ARCHIVO_F)) {
			String linea;
			String anterior = null;
			while ((linea = lector.readLine()) != null) {
				if (!linea.equals(anterior)) {
					escritor.write(linea);
					escritor.newLine();
					anterior = linea;
				}
			}
		}
	}

	static void procesoG() throws IOException {
		long lineas = 0;
		long palabras = 0;
		try (BufferedReader lector = Files.newBufferedReader(ARCHIVO_E, StandardCharsets.UTF_8)) {
			String linea;
			while ((linea = lector.readLine()) != null) {
				lineas++;
				String texto = linea.trim();
				if (!texto.isEmpty()) {
					palabras += texto.split("\\s+").length;
				}
			}
		}
		try (BufferedWriter escritor = crearEscritor(ARCHIVO_G)) {
			escritor.write("Lineas: " + lineas);
			escritor.newLine();
			escritor.write("Palabras: " + palabras);
			escritor.newLine();
		}
	}

	static void procesoH() throws IOException {
		try (BufferedReader lectorF = Files.newBufferedReader(ARCHIVO_F, StandardCharsets.UTF_8);
			 BufferedReader lectorG = Files.newBufferedReader(ARCHIVO_G, StandardCharsets.UTF_8);
			 BufferedWriter escritor = crearEscritor(ARCHIVO_H)) {
			copiarLineas(lectorF, escritor);
			copiarLineas(lectorG, escritor);
		}
	}

	private static void generar(Path archivo, String prefijo, int cantidad) throws IOException {
		int ancho = Integer.toString(cantidad).length();
		try (BufferedWriter escritor = crearEscritor(archivo)) {
			for (int indice = 1; indice <= cantidad; indice++) {
				String numero = Integer.toString(indice);
				escritor.write(prefijo + " dato " + "0".repeat(ancho - numero.length()) + numero);
				escritor.newLine();
			}
		}
	}

	private static void copiarLineas(BufferedReader lector, BufferedWriter escritor) throws IOException {
		String linea;
		while ((linea = lector.readLine()) != null) {
			escritor.write(linea);
			escritor.newLine();
		}
	}

	private static BufferedWriter crearEscritor(Path archivo) throws IOException {
		return Files.newBufferedWriter(archivo, StandardCharsets.UTF_8,
				StandardOpenOption.CREATE, StandardOpenOption.TRUNCATE_EXISTING, StandardOpenOption.WRITE);
	}
}

// Ejecuta los ocho procesos en orden o según el grafo de dependencias.
class Ejecuciones {
	static void serial(int cantidad) {
		ejecutar("A", () -> ProcesosArchivos.procesoA(cantidad));
		ejecutar("B", () -> ProcesosArchivos.procesoB(cantidad));
		ejecutar("C", ProcesosArchivos::procesoC);
		ejecutar("D", ProcesosArchivos::procesoD);
		ejecutar("E", ProcesosArchivos::procesoE);
		ejecutar("F", ProcesosArchivos::procesoF);
		ejecutar("G", ProcesosArchivos::procesoG);
		ejecutar("H", ProcesosArchivos::procesoH);
	}

	static void paralela(int cantidad) {
		ExecutorService executor = Executors.newFixedThreadPool(Main.HILOS);
		try {
			CompletableFuture<Void> procesoA = asincrono(executor, "A", () -> ProcesosArchivos.procesoA(cantidad));
			CompletableFuture<Void> procesoB = asincrono(executor, "B", () -> ProcesosArchivos.procesoB(cantidad));
			CompletableFuture<Void> procesoC = CompletableFuture.allOf(procesoA, procesoB)
					.thenRunAsync(() -> ejecutar("C", ProcesosArchivos::procesoC), executor);
			CompletableFuture<Void> procesoD = procesoC.thenRunAsync(
					() -> ejecutar("D", ProcesosArchivos::procesoD), executor);
			CompletableFuture<Void> procesoE = procesoC.thenRunAsync(
					() -> ejecutar("E", ProcesosArchivos::procesoE), executor);
			CompletableFuture<Void> procesoF = procesoD.thenRunAsync(
					() -> ejecutar("F", ProcesosArchivos::procesoF), executor);
			CompletableFuture<Void> procesoG = procesoE.thenRunAsync(
					() -> ejecutar("G", ProcesosArchivos::procesoG), executor);
			CompletableFuture<Void> procesoH = CompletableFuture.allOf(procesoF, procesoG)
					.thenRunAsync(() -> ejecutar("H", ProcesosArchivos::procesoH), executor);
			procesoH.join();
		} finally {
			executor.shutdown();
		}
	}

	private static CompletableFuture<Void> asincrono(ExecutorService executor, String nodo, TareaArchivo tarea) {
		return CompletableFuture.runAsync(() -> ejecutar(nodo, tarea), executor);
	}

	private static void ejecutar(String nodo, TareaArchivo tarea) {
		long inicio = System.nanoTime();
		try {
			tarea.ejecutar();
		} catch (IOException error) {
			throw new UncheckedIOException(error);
		} finally {
			double duracionMs = (System.nanoTime() - inicio) / 1_000_000.0;
			System.out.printf("%s | %s | %.3f ms%n", nodo, Thread.currentThread().getName(), duracionMs);
		}
	}

	@FunctionalInterface
	private interface TareaArchivo {
		void ejecutar() throws IOException;
	}
}
