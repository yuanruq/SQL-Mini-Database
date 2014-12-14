/*-
 * See the file LICENSE for redistribution information.
 *
 * Copyright (c) 2002, 2014 Oracle and/or its affiliates. All rights reserved.
 *
 */

import java.util.Map;

import com.sun.javadoc.ClassDoc;
import com.sun.javadoc.MethodDoc;
import com.sun.javadoc.Type;
import com.sun.javadoc.AnnotationTypeElementDoc;

class HidingMethodDocWrapper extends HidingExecutableMemberDocWrapper
                             implements MethodDoc {
    public HidingMethodDocWrapper(MethodDoc methdoc, Map mapWrappers) {
        super(methdoc, mapWrappers);
    }

    private MethodDoc _getMethodDoc() {
        return (MethodDoc)getWrappedObject();
    }

    public boolean isAbstract() {
        return _getMethodDoc().isAbstract();
    }

    public ClassDoc overriddenClass() {
        return (ClassDoc)wrapOrHide(_getMethodDoc().overriddenClass());
    }

    public MethodDoc overriddenMethod() {
        return (MethodDoc)wrapOrHide(_getMethodDoc().overriddenMethod());
    }

    public Type returnType() {
        return (Type)wrapOrHide(_getMethodDoc().returnType());
    }

    public boolean overrides(MethodDoc meth) {
       if (meth instanceof HidingAnnotationTypeElementDocWrapper) {
           meth = (AnnotationTypeElementDoc)
                  ((HidingAnnotationTypeElementDocWrapper)meth).
                  getWrappedObject();
       } else if (meth instanceof HidingMethodDocWrapper) {
           meth  = (MethodDoc)
                   ((HidingMethodDocWrapper)meth).getWrappedObject();
       }

       return _getMethodDoc().overrides((MethodDoc) meth);
    }

    public Type overriddenType() {
        return (Type)wrapOrHide(_getMethodDoc().overriddenType());
    }

    public boolean isVarAgrs() {
        return _getMethodDoc().isVarArgs();
    }

    public Type[] thrownExceptionTypes() {
        return (Type[]) wrapOrHide(_getMethodDoc().thrownExceptionTypes());
    }
}
