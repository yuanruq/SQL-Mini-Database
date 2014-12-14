/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.io.File;
import java.util.Map;

import com.sun.javadoc.SourcePosition;

class HidingSourcePositionWrapper extends HidingWrapper
                                       implements SourcePosition {
    public HidingSourcePositionWrapper(SourcePosition type, Map mapWrappers) {
        super(type, mapWrappers);
    }

    private SourcePosition _getSourcePosition() {
        return (SourcePosition)getWrappedObject();
    }

    public int column() {
        return _getSourcePosition().column();
    }

    public File file() {
        return _getSourcePosition().file();
    }

    public int line() {
        return _getSourcePosition().line();
    }

    public String toString() {
        return _getSourcePosition().toString();
    }
}
