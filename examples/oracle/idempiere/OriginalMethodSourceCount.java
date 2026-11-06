/* Counts a trusted original static collection provider; does not execute tests. */
import java.lang.reflect.*;
import java.nio.file.*;
import java.util.*;
import org.junit.jupiter.params.ParameterizedTest;
import org.junit.jupiter.params.provider.MethodSource;

class OriginalMethodSourceCount {
 public static void main(String[] args) throws Exception {
  if (args.length != 3) throw new IllegalArgumentException("class method expected-jar");
  Class<?> type = Class.forName(args[0]);
  Path origin = Paths.get(type.getProtectionDomain().getCodeSource().getLocation().toURI()).toRealPath();
  if (!origin.equals(Paths.get(args[2]).toRealPath())) throw new IllegalStateException("Unexpected class origin");
  Method test = null;
  for (Method method : type.getDeclaredMethods()) {
   if (method.getName().equals(args[1])) {
    if (test != null) throw new IllegalStateException("Overloaded test requires explicit selection");
    test = method;
   }
  }
  if (test == null || test.getAnnotation(ParameterizedTest.class) == null) throw new IllegalStateException("Expected parameterized test");
  MethodSource source = test.getAnnotation(MethodSource.class);
  if (source == null || source.value().length != 1) throw new IllegalStateException("Expected one explicit method source");
  Method provider = type.getDeclaredMethod(source.value()[0]);
  if (!Modifier.isStatic(provider.getModifiers())) throw new IllegalStateException("Expected static provider");
  Object value = provider.invoke(null);
  if (!(value instanceof Collection<?> rows) || rows.isEmpty()) throw new IllegalStateException("Expected nonempty collection");
  for (Object row : rows) {
   if (!(row instanceof Object[] arguments) || arguments.length != test.getParameterCount()) throw new IllegalStateException("Argument arity mismatch");
  }
  System.out.println("class="+type.getName());
  System.out.println("provider="+provider.getName());
  System.out.println("invocations="+((Collection<?>)value).size());
  System.out.println("origin="+origin);
 }
}
