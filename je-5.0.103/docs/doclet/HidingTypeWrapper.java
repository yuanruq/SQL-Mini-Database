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
import com.sun.javadoc.Type;
import com.sun.javadoc.TypeVariable;
import com.sun.javadoc.WildcardType;

class HidingTypeWrapper extends HidingWrapper implements Type {
    public HidingTypeWrapper(Type type, Map mapWrappers) {
        super(type, mapWrappers);
    }

    private Type _getType() {
        return (Type)getWrappedObject();
    }

    public ClassDoc asClassDoc() {
        return (ClassDoc)wrapOrHide(_getType().asClassDoc());
    }

    public String dimension() {
        return _getType().dimension();
    }

    public String qualifiedTypeName() {
        return _getType().qualifiedTypeName();
    }

    public String toString() {
        return _getType().toString();
    }

    public String typeName() {
        return _getType().typeName();
    }

    public AnnotationTypeDoc asAnnotationTypeDoc() {
        return (AnnotationTypeDoc)wrapOrHide(_getType().asAnnotationTypeDoc());
    }

    public WildcardType asWildcardType() {
        return (WildcardType)wrapOrHide(_getType().asWildcardType()); 
    }

    public TypeVariable asTypeVariable() {
        return (TypeVariable)wrapOrHide(_getType().asTypeVariable());
    }

    public ParameterizedType asParameterizedType() {
        return (ParameterizedType)wrapOrHide(_getType().asParameterizedType());
    }

    public boolean isPrimitive() {
        return _getType().isPrimitive();
    }

    public String simpleTypeName() {
        return _getType().simpleTypeName();
    }
}
