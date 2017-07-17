/*
 * myDebug.h
 *
 *  Created on: 13 Jul 2017
 *      Author: nuritm
 */

#ifndef CORE_MYDEBUG_H_
#define CORE_MYDEBUG_H_

#ifdef USE_GDB
#define GDB(a) a
#else
#define GDB(a)
#endif

#ifdef DEB_PRINT
#define DEB(a) a
#else
#define DEB(a)
#endif


#endif /* CORE_MYDEBUG_H_ */
