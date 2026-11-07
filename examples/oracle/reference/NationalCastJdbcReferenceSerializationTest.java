public final class NationalCastJdbcReferenceSerializationTest {
  static void equal(String expected,String actual) {
    if(!expected.equals(actual))throw new AssertionError("Serialized code-unit contract differs");
  }
  public static void main(String[] args) {
    equal("null",NationalCastJdbcReference.units(null));
    equal("[]",NationalCastJdbcReference.units(""));
    equal("[\"D83D\"]",NationalCastJdbcReference.units(new String(new char[]{(char)0xd83d})));
    equal("[\"DE00\"]",NationalCastJdbcReference.units(new String(new char[]{(char)0xde00})));
    equal("[\"D83D\",\"DE00\"]",NationalCastJdbcReference.units(new String(Character.toChars(0x1f600))));
    equal("[\"754C\",\"0020\",\"0020\"]",NationalCastJdbcReference.units("界  "));
    equal("\"\\ud83d\"",NationalCastJdbcReference.quote(new String(new char[]{(char)0xd83d})));
    equal("\"\\\"\\\\\\u000a\"",NationalCastJdbcReference.quote("\"\\\n"));
    System.out.println("8 serialization controls passed; no reference database used");
  }
}
