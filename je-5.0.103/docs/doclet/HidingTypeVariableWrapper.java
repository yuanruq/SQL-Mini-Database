/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.AnnotationTypeDoc;
import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.ParameterizedType;
import com.sun.javadoc.ProgramElementDoc;
import com.sun.javadoc.Type;
import com.sun.javadoc.TypeVariable;
import com.sun.javadoc.WildcardType;

class HidingTypeVariableWrapper extends HidingWrapper implements TypeVariable {
    public HidingTypeVariableWrapper(TypeVariable type, Map mapWrappers) {
        super(type, mapWrappers);
    }

    private TypeVariable _getTypeVariable() {
        return (TypeVariable)getWrappedObject();
    }

    public Type[] bounds() {
        return (Type[]) wrapOrHide(_getTypeVariable().bounds());
    }

    public ProgramElementDoc owner() {
        return (ProgramElementDoc) wrapOrHide(_getTypeVariable().owner());
    }
    
    public ClassDoc asClassDoc() {
        return (ClassDoc)wrapOrHide(_getTypeVariable().asClassDoc());
    }

    public String dimension() {
        return _getTypeVariable().dimension();
    }

    public String qualifiedTypeName() {
        return _getTypeVariable().qualifiedTypeName();
    }

    public String toString() {
        return _getTypeVariable().toString();
    }

    public String typeName() {
        return _getTypeVariable().typeName();
    }

    public AnnotationTypeDoc asAnnotationTypeDoc() {
        return (AnnotationTypeDoc)
                wrapOrHide(_getTypeVariable().asAnnotationTypeDoc());
    }

    public WildcardType asWildcardType() {
        return (WildcardType)wrapOrHide(_getTypeVariable().asWildcardType());
    }

    public TypeVariable asTypeVariable() {
        return (TypeVariable)wrapOrHide(_getTypeVariable().asTypeVariable());
    }

    public ParameterizedType asParameterizedType() {
        return (ParameterizedType)
                wrapOrHide(_getTypeVariable().asParameterizedType());
    }

    public boolean isPrimitive() {
        return _getTypeVariable().isPrimitive();
    }

    public String simpleTypeName() {
        return _getTypeVariable().simpleTypeName();
    }     
}
