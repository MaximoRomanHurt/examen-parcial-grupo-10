import java.io.BufferedReader;
import java.io.BufferedWriter;
import java.io.IOException;
import java.io.UncheckedIOException;
import java.nio.charset.StandardCharsets;
import java.nio.file.Files;
import java.nio.file.Path;
import java.nio.file.StandardOpenOption;
import java.util.List;
import java.util.concurrent.ExecutionException;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.function.Consumer;
import javax.swing.JButton;
import javax.swing.JFrame;
import javax.swing.JLabel;
import javax.swing.JOptionPane;
import javax.swing.JPanel;
import javax.swing.JProgressBar;
import javax.swing.JScrollPane;
import javax.swing.JTextArea;
import javax.swing.JTextField;
import javax.swing.SwingWorker;
import javax.swing.SwingUtilities;
import java.awt.BorderLayout;
import java.awt.FlowLayout;
import java.awt.Font;

// Ejecuta los procesos de archivos en serie y con dependencias paralelas.
class Main {
	private static final int CANTIDAD_POR_DEFECTO = 1000_000;
	static final int HILOS = 2;

	public static void main(String[] args) throws IOException {
		if (args.length == 0) {
			SwingUtilities.invokeLater(Main::mostrarVentana);
			return;
		}

		int cantidad = Integer.parseInt(args[0]);
		if (cantidad < 1) {
			throw new IllegalArgumentException("N debe ser mayor que cero");
		}
		Files.createDirectories(Path.of("datos-ejercicio-1"));
		ejecutarProcesos(cantidad, System.out::println);
	}

	private static void mostrarVentana() {
		JFrame ventana = new JFrame("Procesamiento de archivos");
		ventana.setDefaultCloseOperation(JFrame.EXIT_ON_CLOSE);
		ventana.setSize(620, 440);
		ventana.setLocationRelativeTo(null);

		JTextField campoCantidad = new JTextField(Integer.toString(CANTIDAD_POR_DEFECTO), 10);
		JButton botonEjecutar = new JButton("Ejecutar");
		JProgressBar progreso = new JProgressBar(0, 16);
		progreso.setStringPainted(true);
		progreso.setString("0 / 16 procesos");
		JTextArea registro = new JTextArea();
		registro.setEditable(false);
		registro.setFont(new Font(Font.MONOSPACED, Font.PLAIN, 12));

		JPanel controles = new JPanel(new FlowLayout(FlowLayout.LEFT));
		controles.add(new JLabel("Líneas por archivo:"));
		controles.add(campoCantidad);
		controles.add(botonEjecutar);

		JPanel contenido = new JPanel(new BorderLayout(8, 8));
		contenido.add(controles, BorderLayout.NORTH);
		contenido.add(new JScrollPane(registro), BorderLayout.CENTER);
		contenido.add(progreso, BorderLayout.SOUTH);
		ventana.setContentPane(contenido);

		botonEjecutar.addActionListener(evento -> {
			int cantidad;
			try {
				cantidad = Integer.parseInt(campoCantidad.getText().trim());
				if (cantidad < 1) {
					throw new NumberFormatException();
				}
			} catch (NumberFormatException error) {
				JOptionPane.showMessageDialog(ventana, "N debe ser un entero mayor que cero.",
						"Valor no válido", JOptionPane.ERROR_MESSAGE);
				return;
			}

			botonEjecutar.setEnabled(false);
			registro.setText("");
			progreso.setValue(0);
			progreso.setString("0 / 16 procesos");
			new SwingWorker<Void, String>() {
				private int procesosTerminados;

				@Override
				protected Void doInBackground() throws Exception {
					Files.createDirectories(Path.of("datos-ejercicio-1"));
					ejecutarProcesos(cantidad, linea -> publish(linea));
					return null;
				}

				@Override
				protected void process(List<String> lineas) {
					for (String linea : lineas) {
						registro.append(linea + System.lineSeparator());
						if (linea.contains(" | ")) {
							procesosTerminados++;
							progreso.setValue(procesosTerminados);
							progreso.setString(procesosTerminados + " / 16 procesos");
						}
					}
					registro.setCaretPosition(registro.getDocument().getLength());
				}

				@Override
				protected void done() {
					botonEjecutar.setEnabled(true);
					try {
						get();
					} catch (InterruptedException error) {
						Thread.currentThread().interrupt();
						JOptionPane.showMessageDialog(ventana, "La ejecución fue interrumpida.");
					} catch (ExecutionException error) {
						JOptionPane.showMessageDialog(ventana, error.getCause().getMessage(),
								"Error de ejecución", JOptionPane.ERROR_MESSAGE);
					}
				}
			}.execute();
		});

		ventana.setVisible(true);
	}

	private static void ejecutarProcesos(int cantidad, Consumer<String> registrar) throws IOException {

		long inicioSerial = System.nanoTime();
		Ejecuciones.serial(cantidad, registrar);
		long tiempoSerial = System.nanoTime() - inicioSerial;

		long inicioParalelo = System.nanoTime();
		Ejecuciones.paralela(cantidad, registrar);
		long tiempoParalelo = System.nanoTime() - inicioParalelo;

		double serialMs = tiempoSerial / 1_000_000.0;
		double paraleloMs = tiempoParalelo / 1_000_000.0;
		double speedup = (double) tiempoSerial / tiempoParalelo;
		double eficiencia = speedup / HILOS * 100.0;
		registrar.accept(String.format("Tiempo serial: %.3f ms", serialMs));
		registrar.accept(String.format("Tiempo paralelo: %.3f ms", paraleloMs));
		registrar.accept(String.format("Speedup: %.3f", speedup));
		registrar.accept(String.format("Eficiencia: %.2f%%", eficiencia));
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
	static void serial(int cantidad, Consumer<String> registrar) {
		ejecutar("A", () -> ProcesosArchivos.procesoA(cantidad), registrar);
		ejecutar("B", () -> ProcesosArchivos.procesoB(cantidad), registrar);
		ejecutar("C", ProcesosArchivos::procesoC, registrar);
		ejecutar("D", ProcesosArchivos::procesoD, registrar);
		ejecutar("E", ProcesosArchivos::procesoE, registrar);
		ejecutar("F", ProcesosArchivos::procesoF, registrar);
		ejecutar("G", ProcesosArchivos::procesoG, registrar);
		ejecutar("H", ProcesosArchivos::procesoH, registrar);
	}

	static void paralela(int cantidad, Consumer<String> registrar) {
		ExecutorService executor = Executors.newFixedThreadPool(Main.HILOS);
		try {
			CompletableFuture<Void> procesoA = asincrono(executor, "A", () -> ProcesosArchivos.procesoA(cantidad), registrar);
			CompletableFuture<Void> procesoB = asincrono(executor, "B", () -> ProcesosArchivos.procesoB(cantidad), registrar);
			CompletableFuture<Void> procesoC = CompletableFuture.allOf(procesoA, procesoB)
					.thenRunAsync(() -> ejecutar("C", ProcesosArchivos::procesoC, registrar), executor);
			CompletableFuture<Void> procesoD = procesoC.thenRunAsync(
					() -> ejecutar("D", ProcesosArchivos::procesoD, registrar), executor);
			CompletableFuture<Void> procesoE = procesoC.thenRunAsync(
					() -> ejecutar("E", ProcesosArchivos::procesoE, registrar), executor);
			CompletableFuture<Void> procesoF = procesoD.thenRunAsync(
					() -> ejecutar("F", ProcesosArchivos::procesoF, registrar), executor);
			CompletableFuture<Void> procesoG = procesoE.thenRunAsync(
					() -> ejecutar("G", ProcesosArchivos::procesoG, registrar), executor);
			CompletableFuture<Void> procesoH = CompletableFuture.allOf(procesoF, procesoG)
					.thenRunAsync(() -> ejecutar("H", ProcesosArchivos::procesoH, registrar), executor);
			procesoH.join();
		} finally {
			executor.shutdown();
		}
	}

	private static CompletableFuture<Void> asincrono(ExecutorService executor, String nodo, TareaArchivo tarea,
			Consumer<String> registrar) {
		return CompletableFuture.runAsync(() -> ejecutar(nodo, tarea, registrar), executor);
	}

	private static void ejecutar(String nodo, TareaArchivo tarea, Consumer<String> registrar) {
		long inicio = System.nanoTime();
		try {
			tarea.ejecutar();
		} catch (IOException error) {
			throw new UncheckedIOException(error);
		} finally {
			double duracionMs = (System.nanoTime() - inicio) / 1_000_000.0;
			registrar.accept(String.format("%s | %s | %.3f ms", nodo, Thread.currentThread().getName(), duracionMs));
		}
	}

	@FunctionalInterface
	private interface TareaArchivo {
		void ejecutar() throws IOException;
	}
}
