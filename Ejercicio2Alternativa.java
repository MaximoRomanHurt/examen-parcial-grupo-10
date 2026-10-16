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
import java.util.ArrayList;
import java.util.Comparator;
import java.util.List;
import java.util.Locale;
import java.util.Random;
import java.util.concurrent.CompletableFuture;
import java.util.concurrent.ExecutorService;
import java.util.concurrent.Executors;
import java.util.concurrent.TimeUnit;
import java.util.stream.Collectors;
import java.util.stream.Stream;

/*
 * PROBLEMA 2: PARALELISMO ORIENTADO A DATOS
 *
 * Caso planteado:
 *   Una universidad posee un repositorio central de mediciones horarias
 *   de consumo eléctrico de sus edificios inteligentes.
 *
 * Arquitectura simulada:
 *
 *   Nodo 1 ─┐
 *   Nodo 2 ─┼──> Application Server / Balanceador ───> DB Server
 *   Nodo 3 ─┤                                      (Repositorio en disco)
 *   Nodo 4 ─┘
 *
 * Restricción:
 *   - El dataset se genera directamente en archivos CSV.
 *   - Los registros se procesan leyendo línea por línea.
 *   - Nunca se carga el dataset completo en memoria.
 *   - Solo se guardan en memoria metadatos pequeños:
 *     rutas, tamaños de archivos, asignaciones y resultados estadísticos.
 *
 * Compilar:
 *   javac Main.java
 *
 * Ejecutar con valores por defecto:
 *   java Main
 *
 * Ejecutar indicando número de archivos y registros por archivo:
 *   java Main 20 30000
 */
public class Ejercicio2Alternativa {

    private static final int ARCHIVOS_POR_DEFECTO = 16;
    private static final int REGISTROS_POR_ARCHIVO_POR_DEFECTO = 25_000;
    private static final int NUMERO_NODOS = 4;

    private static final long MB = 1024L * 1024L;

    private static final Path DIRECTORIO_BASE = Path.of("datos-ejercicio-2");
    private static final Path REPOSITORIO_CENTRAL =
            DIRECTORIO_BASE.resolve("repositorio-central-db-server");

    private static final Path REPORTE_SERIAL =
            DIRECTORIO_BASE.resolve("reporte_serial.txt");

    private static final Path REPORTE_PARALELO =
            DIRECTORIO_BASE.resolve("reporte_paralelo.txt");

    public static void main(String[] args) {
        try {
            Configuracion configuracion = Configuracion.desdeArgumentos(args);

            prepararDirectorios();

            mostrarEncabezado(configuracion);

            long inicioGeneracion = System.nanoTime();

            GeneradorRepositorio.generarRepositorio(
                    REPOSITORIO_CENTRAL,
                    configuracion.getCantidadArchivos(),
                    configuracion.getRegistrosPorArchivo()
            );

            double tiempoGeneracionMs = convertirAMilisegundos(
                    System.nanoTime() - inicioGeneracion
            );

            List<ArchivoTrabajo> archivosRepositorio =
                    RepositorioCentral.obtenerArchivos(REPOSITORIO_CENTRAL);

            long bytesTotales = calcularBytesTotales(archivosRepositorio);

            System.out.printf(
                    Locale.US,
                    "%nRepositorio generado correctamente.%n"
                            + "Archivos: %d%n"
                            + "Tamaño total: %.2f MB%n"
                            + "Tiempo de generación: %.3f ms%n%n",
                    archivosRepositorio.size(),
                    convertirAMegabytes(bytesTotales),
                    tiempoGeneracionMs
            );

            ejecutarVersionSerial(archivosRepositorio);

            ejecutarVersionParalela(archivosRepositorio, bytesTotales);

        } catch (IOException error) {
            System.err.println("Error de entrada/salida: " + error.getMessage());
        } catch (InterruptedException error) {
            Thread.currentThread().interrupt();
            System.err.println("La ejecución fue interrumpida.");
        } catch (IllegalArgumentException error) {
            System.err.println("Error de configuración: " + error.getMessage());
        }
    }

    private static void ejecutarVersionSerial(
            List<ArchivoTrabajo> archivosRepositorio
    ) throws IOException {
        System.out.println("==================================================");
        System.out.println("EJECUCIÓN SERIAL");
        System.out.println("==================================================");

        long inicioSerial = System.nanoTime();

        ResumenProcesamiento resumenSerial =
                ProcesadorDataset.procesarArchivos(archivosRepositorio);

        double tiempoSerialMs = convertirAMilisegundos(
                System.nanoTime() - inicioSerial
        );

        EscritorReportes.escribirReporteSerial(
                REPORTE_SERIAL,
                archivosRepositorio,
                resumenSerial,
                tiempoSerialMs
        );

        System.out.printf(
                Locale.US,
                "Tiempo serial: %.3f ms%n",
                tiempoSerialMs
        );

        System.out.printf(
                Locale.US,
                "Registros procesados: %d%n%n",
                resumenSerial.getCantidadRegistros()
        );
    }

    private static void ejecutarVersionParalela(
            List<ArchivoTrabajo> archivosRepositorio,
            long bytesTotales
    ) throws IOException, InterruptedException {
        System.out.println("==================================================");
        System.out.println("EJECUCIÓN PARALELA CON BALANCEO DE CARGA");
        System.out.println("==================================================");

        List<NodoProcesamiento> nodos =
                FabricaNodos.crearNodos(bytesTotales);

        PlanDistribucion plan =
                BalanceadorCarga.crearPlan(archivosRepositorio, nodos);

        System.out.println("Asignación realizada por el Application Server:");
        imprimirPlan(plan);

        long inicioParalelo = System.nanoTime();

        List<ResultadoNodo> resultadosNodos =
                ApplicationServer.ejecutarPlanParalelo(plan);

        double tiempoParaleloMs = convertirAMilisegundos(
                System.nanoTime() - inicioParalelo
        );

        ResumenProcesamiento resumenGlobal =
                combinarResultados(resultadosNodos);

        EscritorReportes.escribirReporteParalelo(
                REPORTE_PARALELO,
                plan,
                resultadosNodos,
                resumenGlobal,
                tiempoParaleloMs
        );

        System.out.printf(
                Locale.US,
                "%nTiempo paralelo: %.3f ms%n",
                tiempoParaleloMs
        );

        System.out.printf(
                Locale.US,
                "Registros procesados: %d%n",
                resumenGlobal.getCantidadRegistros()
        );

        System.out.println();
        System.out.println("Tiempos por nodo:");

        for (ResultadoNodo resultado : resultadosNodos) {
            System.out.printf(
                    Locale.US,
                    "%s | archivos: %d | datos: %.2f MB | tiempo: %.3f ms%n",
                    resultado.getNombreNodo(),
                    resultado.getCantidadArchivos(),
                    convertirAMegabytes(resultado.getBytesProcesados()),
                    resultado.getTiempoMs()
            );
        }

        System.out.println();
        System.out.println("Reportes generados:");
        System.out.println("- " + REPORTE_SERIAL.toAbsolutePath());
        System.out.println("- " + REPORTE_PARALELO.toAbsolutePath());

        System.out.println();
        System.out.println("Para calcular speedup, revise los tiempos");
        System.out.println("serial y paralelo mostrados en consola o en los reportes.");
    }

    private static void prepararDirectorios() throws IOException {
        Files.createDirectories(DIRECTORIO_BASE);
        Files.createDirectories(REPOSITORIO_CENTRAL);

        try (Stream<Path> archivos = Files.list(REPOSITORIO_CENTRAL)) {
            for (Path archivo : archivos.collect(Collectors.toList())) {
                Files.deleteIfExists(archivo);
            }
        }

        Files.deleteIfExists(REPORTE_SERIAL);
        Files.deleteIfExists(REPORTE_PARALELO);
    }

    private static void mostrarEncabezado(Configuracion configuracion) {
        System.out.println("==================================================");
        System.out.println("PARALELISMO ORIENTADO A DATOS");
        System.out.println("Balanceo de carga entre nodos heterogéneos");
        System.out.println("==================================================");
        System.out.println("Caso: consumo eléctrico de edificios universitarios.");
        System.out.println("Archivos CSV a generar: "
                + configuracion.getCantidadArchivos());
        System.out.println("Registros por archivo: "
                + configuracion.getRegistrosPorArchivo());
        System.out.println("Nodos de procesamiento: " + NUMERO_NODOS);
        System.out.println("Repositorio central: "
                + REPOSITORIO_CENTRAL.toAbsolutePath());
    }

    private static void imprimirPlan(PlanDistribucion plan) {
        for (NodoProcesamiento nodo : plan.getNodos()) {
            System.out.printf(
                    Locale.US,
                    "%s | velocidad: %.0f MB/s | capacidad: %.2f MB"
                            + " | asignado: %.2f MB | archivos: %d%n",
                    nodo.getNombre(),
                    nodo.getVelocidadProcesamientoMBps(),
                    convertirAMegabytes(nodo.getCapacidadAlmacenamientoBytes()),
                    convertirAMegabytes(nodo.getBytesAsignados()),
                    nodo.getArchivosAsignados().size()
            );
        }
    }

    private static ResumenProcesamiento combinarResultados(
            List<ResultadoNodo> resultadosNodos
    ) {
        ResumenProcesamiento resumenGlobal = new ResumenProcesamiento();

        for (ResultadoNodo resultado : resultadosNodos) {
            resumenGlobal.combinar(resultado.getResumen());
        }

        return resumenGlobal;
    }

    private static long calcularBytesTotales(
            List<ArchivoTrabajo> archivos
    ) {
        long total = 0;

        for (ArchivoTrabajo archivo : archivos) {
            total += archivo.getTamanioBytes();
        }

        return total;
    }

    private static double convertirAMilisegundos(long nanosegundos) {
        return nanosegundos / 1_000_000.0;
    }

    private static double convertirAMegabytes(long bytes) {
        return bytes / (double) MB;
    }

    static class Configuracion {

        private final int cantidadArchivos;
        private final int registrosPorArchivo;

        private Configuracion(
                int cantidadArchivos,
                int registrosPorArchivo
        ) {
            this.cantidadArchivos = cantidadArchivos;
            this.registrosPorArchivo = registrosPorArchivo;
        }

        static Configuracion desdeArgumentos(String[] args) {
            int cantidadArchivos = ARCHIVOS_POR_DEFECTO;
            int registrosPorArchivo = REGISTROS_POR_ARCHIVO_POR_DEFECTO;

            if (args.length >= 1) {
                cantidadArchivos = convertirAEnteroPositivo(
                        args[0],
                        "La cantidad de archivos"
                );
            }

            if (args.length >= 2) {
                registrosPorArchivo = convertirAEnteroPositivo(
                        args[1],
                        "La cantidad de registros por archivo"
                );
            }

            if (args.length > 2) {
                throw new IllegalArgumentException(
                        "Uso esperado: java Main [cantidadArchivos] "
                                + "[registrosPorArchivo]"
                );
            }

            return new Configuracion(
                    cantidadArchivos,
                    registrosPorArchivo
            );
        }

        private static int convertirAEnteroPositivo(
                String texto,
                String nombreParametro
        ) {
            try {
                int valor = Integer.parseInt(texto);

                if (valor < 1) {
                    throw new IllegalArgumentException(
                            nombreParametro + " debe ser mayor que cero."
                    );
                }

                return valor;
            } catch (NumberFormatException error) {
                throw new IllegalArgumentException(
                        nombreParametro + " debe ser un número entero válido."
                );
            }
        }

        int getCantidadArchivos() {
            return cantidadArchivos;
        }

        int getRegistrosPorArchivo() {
            return registrosPorArchivo;
        }
    }

    /*
     * Simula el DB Server.
     * Genera varios archivos CSV directamente en el repositorio central.
     */
    static class GeneradorRepositorio {

        private static final DateTimeFormatter FORMATO_FECHA =
                DateTimeFormatter.ofPattern("yyyy-MM-dd'T'HH:mm");

        private GeneradorRepositorio() {
        }

        static void generarRepositorio(
                Path repositorio,
                int cantidadArchivos,
                int registrosPorArchivo
        ) throws IOException {
            LocalDateTime fechaInicial =
                    LocalDateTime.of(2026, 1, 1, 0, 0);

            long desplazamientoHoras = 0;

            for (int numeroArchivo = 1;
                 numeroArchivo <= cantidadArchivos;
                 numeroArchivo++) {

                Path archivo = repositorio.resolve(
                        String.format(
                                Locale.US,
                                "consumo_edificio_%03d.csv",
                                numeroArchivo
                        )
                );

                Random random = new Random(20260000L + numeroArchivo);

                generarArchivo(
                        archivo,
                        fechaInicial.plusHours(desplazamientoHoras),
                        registrosPorArchivo,
                        random
                );

                desplazamientoHoras += registrosPorArchivo;
            }
        }

        private static void generarArchivo(
                Path archivo,
                LocalDateTime fechaInicial,
                int registrosPorArchivo,
                Random random
        ) throws IOException {
            try (BufferedWriter escritor = Files.newBufferedWriter(
                    archivo,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            )) {
                escritor.write(
                        "fechaHora,temperaturaCelsius,humedadPorcentaje,"
                                + "personas,equiposEncendidos,consumoKwh"
                );
                escritor.newLine();

                for (int indice = 0;
                     indice < registrosPorArchivo;
                     indice++) {

                    LocalDateTime fechaHora =
                            fechaInicial.plusHours(indice);

                    int hora = fechaHora.getHour();
                    boolean horarioLaboral = hora >= 8 && hora <= 18;

                    double temperatura = calcularTemperatura(hora, random);
                    double humedad = calcularHumedad(temperatura, random);
                    int personas = calcularPersonas(horarioLaboral, random);
                    int equipos = calcularEquipos(personas, random);
                    double consumo = calcularConsumo(
                            temperatura,
                            humedad,
                            personas,
                            equipos,
                            random
                    );

                    escritor.write(String.format(
                            Locale.US,
                            "%s,%.2f,%.2f,%d,%d,%.2f",
                            fechaHora.format(FORMATO_FECHA),
                            temperatura,
                            humedad,
                            personas,
                            equipos,
                            consumo
                    ));

                    escritor.newLine();
                }
            }
        }

        private static double calcularTemperatura(
                int hora,
                Random random
        ) {
            double cicloDiario =
                    8.0 * Math.sin((hora - 8) * Math.PI / 12.0);

            double ruido = random.nextGaussian() * 1.3;

            return limitar(22.0 + cicloDiario + ruido, 12.0, 39.0);
        }

        private static double calcularHumedad(
                double temperatura,
                Random random
        ) {
            double ruido = random.nextGaussian() * 4.0;

            return limitar(
                    84.0 - temperatura * 1.15 + ruido,
                    28.0,
                    95.0
            );
        }

        private static int calcularPersonas(
                boolean horarioLaboral,
                Random random
        ) {
            int base = horarioLaboral ? 150 : 15;
            int variacion = random.nextInt(51) - 25;

            return Math.max(0, base + variacion);
        }

        private static int calcularEquipos(
                int personas,
                Random random
        ) {
            int variacion = random.nextInt(25) - 12;

            return Math.max(
                    10,
                    30 + (int) (personas * 0.60) + variacion
            );
        }

        private static double calcularConsumo(
                double temperatura,
                double humedad,
                int personas,
                int equipos,
                Random random
        ) {
            double consumoBase = 22.0;
            double efectoTemperatura = temperatura * 1.70;
            double efectoHumedad = humedad * 0.10;
            double efectoPersonas = personas * 0.28;
            double efectoEquipos = equipos * 0.72;
            double ruido = random.nextGaussian() * 5.0;

            return Math.max(
                    0.0,
                    consumoBase
                            + efectoTemperatura
                            + efectoHumedad
                            + efectoPersonas
                            + efectoEquipos
                            + ruido
            );
        }

        private static double limitar(
                double valor,
                double minimo,
                double maximo
        ) {
            return Math.max(minimo, Math.min(valor, maximo));
        }
    }

    /*
     * Simula el acceso al repositorio ubicado en el DB Server.
     * Solo obtiene rutas y tamaños de los archivos, no sus registros.
     */
    static class RepositorioCentral {

        private RepositorioCentral() {
        }

        static List<ArchivoTrabajo> obtenerArchivos(
                Path repositorio
        ) throws IOException {
            List<Path> rutas;

            try (Stream<Path> stream = Files.list(repositorio)) {
                rutas = stream
                        .filter(Files::isRegularFile)
                        .filter(path -> path.toString().endsWith(".csv"))
                        .sorted()
                        .collect(Collectors.toList());
            }

            List<ArchivoTrabajo> archivos = new ArrayList<>();

            for (Path ruta : rutas) {
                archivos.add(new ArchivoTrabajo(
                        ruta,
                        Files.size(ruta)
                ));
            }

            return archivos;
        }
    }

    /*
     * Application Server:
     * Planifica qué archivos procesará cada nodo.
     */
    static class BalanceadorCarga {

        private BalanceadorCarga() {
        }

        static PlanDistribucion crearPlan(
                List<ArchivoTrabajo> archivos,
                List<NodoProcesamiento> nodos
        ) {
            List<ArchivoTrabajo> archivosOrdenados =
                    new ArrayList<>(archivos);

            archivosOrdenados.sort(
                    Comparator.comparingLong(
                            ArchivoTrabajo::getTamanioBytes
                    ).reversed()
            );

            for (ArchivoTrabajo archivo : archivosOrdenados) {
                NodoProcesamiento mejorNodo =
                        seleccionarMejorNodo(archivo, nodos);

                if (mejorNodo == null) {
                    throw new IllegalArgumentException(
                            "No existe un nodo con capacidad suficiente "
                                    + "para procesar el archivo: "
                                    + archivo.getRuta().getFileName()
                    );
                }

                mejorNodo.asignarArchivo(archivo);
            }

            return new PlanDistribucion(nodos);
        }

        /*
         * Selecciona el nodo que terminaría antes según:
         *
         * tiempoEstimado = (bytesYaAsignados + bytesNuevoArchivo)
         *                  / velocidadDeProcesamiento
         *
         * También verifica que el archivo no supere la capacidad
         * de almacenamiento disponible del nodo.
         */
        private static NodoProcesamiento seleccionarMejorNodo(
                ArchivoTrabajo archivo,
                List<NodoProcesamiento> nodos
        ) {
            NodoProcesamiento mejorNodo = null;
            double menorTiempoEstimado = Double.MAX_VALUE;

            for (NodoProcesamiento nodo : nodos) {
                if (!nodo.puedeRecibir(archivo)) {
                    continue;
                }

                double tiempoEstimado =
                        nodo.calcularTiempoEstimadoSegundos(archivo);

                if (tiempoEstimado < menorTiempoEstimado) {
                    menorTiempoEstimado = tiempoEstimado;
                    mejorNodo = nodo;
                }
            }

            return mejorNodo;
        }
    }

    /*
     * Ejecuta los nodos en paralelo.
     * Cada nodo procesa secuencialmente sus propios archivos,
     * pero los nodos trabajan al mismo tiempo.
     */
    static class ApplicationServer {

        private ApplicationServer() {
        }

        static List<ResultadoNodo> ejecutarPlanParalelo(
                PlanDistribucion plan
        ) throws InterruptedException {
            ExecutorService executor =
                    Executors.newFixedThreadPool(NUMERO_NODOS);

            try {
                List<CompletableFuture<ResultadoNodo>> tareas =
                        new ArrayList<>();

                for (NodoProcesamiento nodo : plan.getNodos()) {
                    CompletableFuture<ResultadoNodo> tarea =
                            CompletableFuture.supplyAsync(
                                    () -> procesarNodo(nodo),
                                    executor
                            );

                    tareas.add(tarea);
                }

                List<ResultadoNodo> resultados = new ArrayList<>();

                for (CompletableFuture<ResultadoNodo> tarea : tareas) {
                    resultados.add(tarea.join());
                }

                return resultados;

            } finally {
                executor.shutdown();
                executor.awaitTermination(1, TimeUnit.MINUTES);
            }
        }

        private static ResultadoNodo procesarNodo(
                NodoProcesamiento nodo
        ) {
            long inicio = System.nanoTime();

            try {
                ResumenProcesamiento resumen =
                        ProcesadorDataset.procesarArchivos(
                                nodo.getArchivosAsignados()
                        );

                double tiempoMs = convertirAMilisegundos(
                        System.nanoTime() - inicio
                );

                return new ResultadoNodo(
                        nodo.getNombre(),
                        nodo.getArchivosAsignados().size(),
                        nodo.getBytesAsignados(),
                        resumen,
                        tiempoMs
                );

            } catch (IOException error) {
                throw new UncheckedIOException(error);
            }
        }
    }

    /*
     * Cada archivo se lee de forma secuencial.
     * No se usa Files.readAllLines ni estructuras que contengan
     * todos los registros del dataset.
     */
    static class ProcesadorDataset {

        private ProcesadorDataset() {
        }

        static ResumenProcesamiento procesarArchivos(
                List<ArchivoTrabajo> archivos
        ) throws IOException {
            ResumenProcesamiento resumen = new ResumenProcesamiento();

            for (ArchivoTrabajo archivo : archivos) {
                procesarArchivo(archivo.getRuta(), resumen);
            }

            return resumen;
        }

        private static void procesarArchivo(
                Path archivo,
                ResumenProcesamiento resumen
        ) throws IOException {
            try (BufferedReader lector = Files.newBufferedReader(
                    archivo,
                    StandardCharsets.UTF_8
            )) {
                lector.readLine();

                String linea;

                while ((linea = lector.readLine()) != null) {
                    String[] valores = linea.split(",");

                    double temperatura = Double.parseDouble(valores[1]);
                    double humedad = Double.parseDouble(valores[2]);
                    int personas = Integer.parseInt(valores[3]);
                    int equipos = Integer.parseInt(valores[4]);
                    double consumo = Double.parseDouble(valores[5]);

                    resumen.agregarRegistro(
                            temperatura,
                            humedad,
                            personas,
                            equipos,
                            consumo
                    );
                }
            }
        }
    }

    /*
     * Acumula estadísticas sin guardar los registros.
     */
    static class ResumenProcesamiento {

        private long cantidadRegistros;
        private double sumaConsumo;
        private double consumoMinimo = Double.MAX_VALUE;
        private double consumoMaximo = -Double.MAX_VALUE;

        private final AcumuladorPearson temperaturaConsumo =
                new AcumuladorPearson();

        private final AcumuladorPearson personasConsumo =
                new AcumuladorPearson();

        private final AcumuladorPearson equiposConsumo =
                new AcumuladorPearson();

        private final AcumuladorPearson humedadConsumo =
                new AcumuladorPearson();

        void agregarRegistro(
                double temperatura,
                double humedad,
                int personas,
                int equipos,
                double consumo
        ) {
            cantidadRegistros++;
            sumaConsumo += consumo;
            consumoMinimo = Math.min(consumoMinimo, consumo);
            consumoMaximo = Math.max(consumoMaximo, consumo);

            temperaturaConsumo.agregar(temperatura, consumo);
            personasConsumo.agregar(personas, consumo);
            equiposConsumo.agregar(equipos, consumo);
            humedadConsumo.agregar(humedad, consumo);
        }

        void combinar(ResumenProcesamiento otro) {
            cantidadRegistros += otro.cantidadRegistros;
            sumaConsumo += otro.sumaConsumo;

            consumoMinimo = Math.min(consumoMinimo, otro.consumoMinimo);
            consumoMaximo = Math.max(consumoMaximo, otro.consumoMaximo);

            temperaturaConsumo.combinar(otro.temperaturaConsumo);
            personasConsumo.combinar(otro.personasConsumo);
            equiposConsumo.combinar(otro.equiposConsumo);
            humedadConsumo.combinar(otro.humedadConsumo);
        }

        long getCantidadRegistros() {
            return cantidadRegistros;
        }

        double getConsumoPromedio() {
            if (cantidadRegistros == 0) {
                return 0.0;
            }

            return sumaConsumo / cantidadRegistros;
        }

        double getConsumoMinimo() {
            return cantidadRegistros == 0 ? 0.0 : consumoMinimo;
        }

        double getConsumoMaximo() {
            return cantidadRegistros == 0 ? 0.0 : consumoMaximo;
        }

        double getCorrelacionTemperaturaConsumo() {
            return temperaturaConsumo.calcularCorrelacion();
        }

        double getCorrelacionPersonasConsumo() {
            return personasConsumo.calcularCorrelacion();
        }

        double getCorrelacionEquiposConsumo() {
            return equiposConsumo.calcularCorrelacion();
        }

        double getCorrelacionHumedadConsumo() {
            return humedadConsumo.calcularCorrelacion();
        }
    }

    /*
     * Acumulador para la correlación de Pearson.
     * Mantiene solo sumas y contadores.
     */
    static class AcumuladorPearson {

        private long cantidad;
        private double sumaX;
        private double sumaY;
        private double sumaXCuadrado;
        private double sumaYCuadrado;
        private double sumaXY;

        void agregar(double valorX, double valorY) {
            cantidad++;
            sumaX += valorX;
            sumaY += valorY;
            sumaXCuadrado += valorX * valorX;
            sumaYCuadrado += valorY * valorY;
            sumaXY += valorX * valorY;
        }

        void combinar(AcumuladorPearson otro) {
            cantidad += otro.cantidad;
            sumaX += otro.sumaX;
            sumaY += otro.sumaY;
            sumaXCuadrado += otro.sumaXCuadrado;
            sumaYCuadrado += otro.sumaYCuadrado;
            sumaXY += otro.sumaXY;
        }

        double calcularCorrelacion() {
            if (cantidad == 0) {
                return 0.0;
            }

            double numerador = cantidad * sumaXY - sumaX * sumaY;

            double parteX =
                    cantidad * sumaXCuadrado - sumaX * sumaX;

            double parteY =
                    cantidad * sumaYCuadrado - sumaY * sumaY;

            double denominador = Math.sqrt(parteX * parteY);

            if (denominador == 0.0) {
                return 0.0;
            }

            return numerador / denominador;
        }
    }

    /*
     * Define nodos heterogéneos:
     * diferentes velocidades de procesamiento y capacidades de almacenamiento.
     */
    static class FabricaNodos {

        private FabricaNodos() {
        }

        static List<NodoProcesamiento> crearNodos(long bytesTotales) {
            List<NodoProcesamiento> nodos = new ArrayList<>();

            nodos.add(new NodoProcesamiento(
                    "Nodo 1",
                    60.0,
                    Math.max(10 * MB, (long) (bytesTotales * 0.25))
            ));

            nodos.add(new NodoProcesamiento(
                    "Nodo 2",
                    120.0,
                    Math.max(15 * MB, (long) (bytesTotales * 0.45))
            ));

            nodos.add(new NodoProcesamiento(
                    "Nodo 3",
                    90.0,
                    Math.max(12 * MB, (long) (bytesTotales * 0.30))
            ));

            nodos.add(new NodoProcesamiento(
                    "Nodo 4",
                    200.0,
                    Math.max(20 * MB, (long) (bytesTotales * 0.70))
            ));

            return nodos;
        }
    }

    static class NodoProcesamiento {

        private final String nombre;
        private final double velocidadProcesamientoMBps;
        private final long capacidadAlmacenamientoBytes;
        private final List<ArchivoTrabajo> archivosAsignados =
                new ArrayList<>();

        private long bytesAsignados;

        NodoProcesamiento(
                String nombre,
                double velocidadProcesamientoMBps,
                long capacidadAlmacenamientoBytes
        ) {
            this.nombre = nombre;
            this.velocidadProcesamientoMBps = velocidadProcesamientoMBps;
            this.capacidadAlmacenamientoBytes =
                    capacidadAlmacenamientoBytes;
        }

        boolean puedeRecibir(ArchivoTrabajo archivo) {
            return bytesAsignados + archivo.getTamanioBytes()
                    <= capacidadAlmacenamientoBytes;
        }

        void asignarArchivo(ArchivoTrabajo archivo) {
            if (!puedeRecibir(archivo)) {
                throw new IllegalArgumentException(
                        "El nodo no tiene espacio suficiente."
                );
            }

            archivosAsignados.add(archivo);
            bytesAsignados += archivo.getTamanioBytes();
        }

        double calcularTiempoEstimadoSegundos(ArchivoTrabajo archivo) {
            double bytesFinales = bytesAsignados + archivo.getTamanioBytes();
            double megabytesFinales = bytesFinales / (double) MB;

            return megabytesFinales / velocidadProcesamientoMBps;
        }

        String getNombre() {
            return nombre;
        }

        double getVelocidadProcesamientoMBps() {
            return velocidadProcesamientoMBps;
        }

        long getCapacidadAlmacenamientoBytes() {
            return capacidadAlmacenamientoBytes;
        }

        long getBytesAsignados() {
            return bytesAsignados;
        }

        List<ArchivoTrabajo> getArchivosAsignados() {
            return archivosAsignados;
        }
    }

    static class ArchivoTrabajo {

        private final Path ruta;
        private final long tamanioBytes;

        ArchivoTrabajo(Path ruta, long tamanioBytes) {
            this.ruta = ruta;
            this.tamanioBytes = tamanioBytes;
        }

        Path getRuta() {
            return ruta;
        }

        long getTamanioBytes() {
            return tamanioBytes;
        }
    }

    static class PlanDistribucion {

        private final List<NodoProcesamiento> nodos;

        PlanDistribucion(List<NodoProcesamiento> nodos) {
            this.nodos = nodos;
        }

        List<NodoProcesamiento> getNodos() {
            return nodos;
        }
    }

    static class ResultadoNodo {

        private final String nombreNodo;
        private final int cantidadArchivos;
        private final long bytesProcesados;
        private final ResumenProcesamiento resumen;
        private final double tiempoMs;

        ResultadoNodo(
                String nombreNodo,
                int cantidadArchivos,
                long bytesProcesados,
                ResumenProcesamiento resumen,
                double tiempoMs
        ) {
            this.nombreNodo = nombreNodo;
            this.cantidadArchivos = cantidadArchivos;
            this.bytesProcesados = bytesProcesados;
            this.resumen = resumen;
            this.tiempoMs = tiempoMs;
        }

        String getNombreNodo() {
            return nombreNodo;
        }

        int getCantidadArchivos() {
            return cantidadArchivos;
        }

        long getBytesProcesados() {
            return bytesProcesados;
        }

        ResumenProcesamiento getResumen() {
            return resumen;
        }

        double getTiempoMs() {
            return tiempoMs;
        }
    }

    static class EscritorReportes {

        private EscritorReportes() {
        }

        static void escribirReporteSerial(
                Path archivoReporte,
                List<ArchivoTrabajo> archivos,
                ResumenProcesamiento resumen,
                double tiempoMs
        ) throws IOException {
            try (BufferedWriter escritor = crearEscritor(archivoReporte)) {
                escritor.write("REPORTE - EJECUCIÓN SERIAL");
                escritor.newLine();
                escritor.write("==========================================");
                escritor.newLine();
                escritor.write("Arquitectura: un solo procesador lógico.");
                escritor.newLine();
                escritor.write("Archivos procesados: " + archivos.size());
                escritor.newLine();
                escritor.write(String.format(
                        Locale.US,
                        "Tiempo total: %.3f ms",
                        tiempoMs
                ));
                escritor.newLine();
                escritor.newLine();

                escribirResumen(escritor, resumen);
            }
        }

        static void escribirReporteParalelo(
                Path archivoReporte,
                PlanDistribucion plan,
                List<ResultadoNodo> resultadosNodos,
                ResumenProcesamiento resumenGlobal,
                double tiempoParaleloMs
        ) throws IOException {
            try (BufferedWriter escritor = crearEscritor(archivoReporte)) {
                escritor.write("REPORTE - EJECUCIÓN PARALELA");
                escritor.newLine();
                escritor.write("==========================================");
                escritor.newLine();
                escritor.write("Arquitectura: Application Server + DB Server + Nodos.");
                escritor.newLine();
                escritor.write(String.format(
                        Locale.US,
                        "Tiempo total paralelo: %.3f ms",
                        tiempoParaleloMs
                ));
                escritor.newLine();
                escritor.newLine();

                escritor.write("PLAN DE BALANCEO DE CARGA");
                escritor.newLine();
                escritor.write("------------------------------------------");
                escritor.newLine();

                for (NodoProcesamiento nodo : plan.getNodos()) {
                    escritor.write(String.format(
                            Locale.US,
                            "%s | velocidad: %.0f MB/s | capacidad: %.2f MB"
                                    + " | asignado: %.2f MB | archivos: %d",
                            nodo.getNombre(),
                            nodo.getVelocidadProcesamientoMBps(),
                            convertirAMegabytes(
                                    nodo.getCapacidadAlmacenamientoBytes()
                            ),
                            convertirAMegabytes(nodo.getBytesAsignados()),
                            nodo.getArchivosAsignados().size()
                    ));
                    escritor.newLine();
                }

                escritor.newLine();
                escritor.write("RESULTADOS POR NODO");
                escritor.newLine();
                escritor.write("------------------------------------------");
                escritor.newLine();

                for (ResultadoNodo resultado : resultadosNodos) {
                    escritor.write(String.format(
                            Locale.US,
                            "%s | archivos: %d | procesado: %.2f MB"
                                    + " | tiempo: %.3f ms",
                            resultado.getNombreNodo(),
                            resultado.getCantidadArchivos(),
                            convertirAMegabytes(resultado.getBytesProcesados()),
                            resultado.getTiempoMs()
                    ));
                    escritor.newLine();
                }

                escritor.newLine();
                escritor.write("RESULTADO GLOBAL COMBINADO");
                escritor.newLine();
                escritor.write("------------------------------------------");
                escritor.newLine();

                escribirResumen(escritor, resumenGlobal);

                escritor.newLine();
                escritor.write("NOTA DE MEMORIA");
                escritor.newLine();
                escritor.write("------------------------------------------");
                escritor.newLine();
                escritor.write(
                        "Los registros del dataset no fueron cargados "
                                + "completamente en memoria. Cada archivo CSV "
                                + "fue leído secuencialmente, línea por línea."
                );
                escritor.newLine();
            }
        }

        private static void escribirResumen(
                BufferedWriter escritor,
                ResumenProcesamiento resumen
        ) throws IOException {
            escritor.write("Registros procesados: "
                    + resumen.getCantidadRegistros());
            escritor.newLine();

            escritor.write(String.format(
                    Locale.US,
                    "Consumo promedio: %.4f kWh",
                    resumen.getConsumoPromedio()
            ));
            escritor.newLine();

            escritor.write(String.format(
                    Locale.US,
                    "Consumo mínimo: %.4f kWh",
                    resumen.getConsumoMinimo()
            ));
            escritor.newLine();

            escritor.write(String.format(
                    Locale.US,
                    "Consumo máximo: %.4f kWh",
                    resumen.getConsumoMaximo()
            ));
            escritor.newLine();

            escritor.newLine();
            escritor.write("Correlaciones de Pearson:");
            escritor.newLine();

            escritor.write(String.format(
                    Locale.US,
                    "- Temperatura vs consumo: %.4f%n",
                    resumen.getCorrelacionTemperaturaConsumo()
            ));

            escritor.write(String.format(
                    Locale.US,
                    "- Personas vs consumo: %.4f%n",
                    resumen.getCorrelacionPersonasConsumo()
            ));

            escritor.write(String.format(
                    Locale.US,
                    "- Equipos encendidos vs consumo: %.4f%n",
                    resumen.getCorrelacionEquiposConsumo()
            ));

            escritor.write(String.format(
                    Locale.US,
                    "- Humedad vs consumo: %.4f%n",
                    resumen.getCorrelacionHumedadConsumo()
            ));
        }

        private static BufferedWriter crearEscritor(
                Path archivo
        ) throws IOException {
            return Files.newBufferedWriter(
                    archivo,
                    StandardCharsets.UTF_8,
                    StandardOpenOption.CREATE,
                    StandardOpenOption.TRUNCATE_EXISTING,
                    StandardOpenOption.WRITE
            );
        }
    }
}
