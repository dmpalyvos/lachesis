package io.palyvos.scheduler.util;

import com.sun.jna.Library;
import com.sun.jna.Native;

interface CLibrary extends Library {

  CLibrary INSTANCE = (CLibrary) Native.load("c", CLibrary.class);

  int seteuid(int euid);

  int setegid(int egid);

  int getuid();

  int geteuid();
}
